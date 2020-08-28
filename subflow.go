package multipath

import (
	"bufio"
	"io"
	"net"
	"sync"
	"time"

	"github.com/getlantern/ema"
	pool "github.com/libp2p/go-buffer-pool"
)

type pendingAck struct {
	sz     int
	sentAt time.Time
}
type subflow struct {
	to   string
	conn net.Conn
	bc   *mpConn

	chClose       chan struct{}
	closeOnce     sync.Once
	sendQueue     chan sendFrame
	pendingAcks   map[uint64]pendingAck
	muPendingAcks sync.Mutex
	probeStart    time.Time
	emaRTT        *ema.EMA
	rttUpdater    func(time.Duration)
}

func startSubflow(c net.Conn, bc *mpConn, clientSide bool, probeStart time.Time, rttUpdater func(time.Duration)) *subflow {
	sf := &subflow{
		to:          c.RemoteAddr().String(),
		conn:        c,
		bc:          bc,
		chClose:     make(chan struct{}),
		sendQueue:   make(chan sendFrame),
		pendingAcks: make(map[uint64]pendingAck),
		emaRTT:      ema.NewDuration(time.Second, 0.1),
		rttUpdater:  rttUpdater,
	}
	if clientSide {
		initialRTT := time.Since(probeStart)
		rttUpdater(initialRTT)
		sf.emaRTT.SetDuration(initialRTT)
		// pong immediately so the server can calculate the RTT between when it
		// sends the leading bytes and receives the pong frame.
		sf.ack(frameTypePong)
	} else {
		sf.probeStart = probeStart
	}
	go func() {
		if err := sf.readLoop(); err != nil {
			log.Debugf("read loop to %v ended: %v", sf.to, err)
		}
	}()
	go sf.sendLoop()
	return sf
}

func (sf *subflow) readLoop() (err error) {
	ch := make(chan *frame)
	r := bufio.NewReader(sf.conn)
	go func() {
		defer close(ch)
		for {
			sz, err := ReadVarInt(r)
			if err != nil {
				sf.close()
				return
			}
			fn, err := ReadVarInt(r)
			if err != nil {
				sf.close()
				return
			}
			if sz == 0 {
				sf.gotACK(fn)
				continue
			}
			buf := pool.Get(int(sz))
			_, err = io.ReadFull(r, buf)
			if err != nil {
				pool.Put(buf)
				sf.close()
				return
			}
			sf.ack(fn)
			ch <- &frame{fn: fn, bytes: buf}
			select {
			case <-sf.chClose:
				return
			default:
			}
		}
	}()
	probeTimer := time.NewTimer(probeInterval)
	for {
		select {
		case frame := <-ch:
			if frame == nil {
				return
			}
			sf.bc.recvQueue.add(frame)
			if !probeTimer.Stop() {
				<-probeTimer.C
			}
			probeTimer.Reset(probeInterval)
		case <-probeTimer.C:
			sf.probe()
		}
	}
}

func (sf *subflow) sendLoop() {
	for frame := range sf.sendQueue {
		n, err := sf.conn.Write(frame.buf)
		if err != nil {
			log.Debugf("closing subflow to %v: %v", sf.to, err)
			sf.close()
			sf.bc.retransmit(frame)
		}
		log.Tracef("Done writing %v bytes to wire", n)
		if frame.isDataFrame {
			now := time.Now()
			sf.muPendingAcks.Lock()
			sf.pendingAcks[frame.fn] = pendingAck{len(frame.buf), now}
			sf.muPendingAcks.Unlock()
			frameCopy := frame // to avoid race
			time.AfterFunc(sf.retransTimer(), func() {
				if sf.isPendingAck(frameCopy.fn) {
					sf.bc.retransmit(frameCopy)
				}
			})
		}
	}
}

func (sf *subflow) ack(fn uint64) {
	log.Tracef("acking frame# %v", fn)
	frame := composeFrame(fn, nil)
	_, err := sf.conn.Write(frame.buf)
	if err != nil {
		sf.close()
	}
	pool.Put(frame.buf)
}

func (sf *subflow) gotACK(fn uint64) {
	if fn == frameTypePing {
		sf.ack(frameTypePong)
		return
	}
	if fn == frameTypePong && !sf.probeStart.IsZero() {
		rtt := time.Since(sf.probeStart)
		sf.rttUpdater(rtt)
		sf.emaRTT.UpdateDuration(rtt)
		sf.probeStart = time.Time{}
		return
	}
	log.Tracef("Got ack for frame# %v", fn)
	sf.muPendingAcks.Lock()
	defer sf.muPendingAcks.Unlock()
	pendingAck, found := sf.pendingAcks[fn]
	if found && pendingAck.sz < maxFrameSizeToCalculateRTT {
		// it's okay to calculate RTT this way because ack frame is always sent
		// back through the same subflow, and a data frame is never sent over
		// the same subflow more than once.
		rtt := time.Since(pendingAck.sentAt)
		sf.rttUpdater(rtt)
		sf.emaRTT.UpdateDuration(rtt)
		delete(sf.pendingAcks, fn)
	}
}

func (sf *subflow) isPendingAck(fn uint64) bool {
	sf.muPendingAcks.Lock()
	defer sf.muPendingAcks.Unlock()
	_, found := sf.pendingAcks[fn]
	return found
}

func (sf *subflow) probe() {
	sf.ack(frameTypePing)
	sf.probeStart = time.Now()
}

func (sf *subflow) retransTimer() time.Duration {
	d := sf.emaRTT.GetDuration() * 2
	if d < 100*time.Millisecond {
		d = 100 * time.Millisecond
	}
	return d
}

func (sf *subflow) close() {
	sf.closeOnce.Do(func() {
		log.Debugf("closing subflow to %v", sf.to)
		sf.bc.remove(sf)
		sf.conn.Close()
		close(sf.sendQueue)
		close(sf.chClose)
	})
}
