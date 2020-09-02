package multipath

import (
	"bufio"
	"io"
	"net"
	"sync"
	"sync/atomic"
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
	mpc  *mpConn

	chClose       chan struct{}
	closeOnce     sync.Once
	sendQueue     chan sendFrame
	pendingAcks   map[uint64]pendingAck
	muPendingAcks sync.Mutex
	probeStart    atomic.Value // time.Time
	emaRTT        *ema.EMA
	tracker       statsTracker
}

func startSubflow(to string, c net.Conn, mpc *mpConn, clientSide bool, probeStart time.Time, tracker statsTracker) *subflow {
	sf := &subflow{
		to:          to,
		conn:        c,
		mpc:         mpc,
		chClose:     make(chan struct{}),
		sendQueue:   make(chan sendFrame),
		pendingAcks: make(map[uint64]pendingAck),
		emaRTT:      ema.NewDuration(longRTT, 0.1),
		tracker:     tracker,
	}
	if clientSide {
		initialRTT := time.Since(probeStart)
		tracker.updateRTT(initialRTT)
		sf.emaRTT.SetDuration(initialRTT)
		// pong immediately so the server can calculate the RTT between when it
		// sends the leading bytes and receives the pong frame.
		sf.ack(frameTypePong)
	} else {
		sf.probeStart.Store(probeStart)
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
			log.Tracef("got frame# %v from %v with %v bytes", fn, sf.to, sz)
			buf := pool.Get(int(sz))
			_, err = io.ReadFull(r, buf)
			if err != nil {
				pool.Put(buf)
				sf.close()
				return
			}
			sf.ack(fn)
			ch <- &frame{fn: fn, bytes: buf}
			sf.tracker.onRecv(sz)
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
			sf.mpc.recvQueue.add(frame)
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
	for {
		select {
		case <-sf.chClose:
			return
		case frame := <-sf.sendQueue:
			n, err := sf.conn.Write(frame.buf)
			if err != nil {
				sf.close()
				sf.mpc.retransmit(frame)
			}
			if frame.retransmissions == 0 {
				sf.tracker.onSent(frame.sz)
			} else {
				sf.tracker.onRetransmit(frame.sz)
			}
			log.Tracef("Done writing %v bytes to wire", n)
			if frame.isDataFrame() {
				now := time.Now()
				sf.muPendingAcks.Lock()
				sf.pendingAcks[frame.fn] = pendingAck{len(frame.buf), now}
				sf.muPendingAcks.Unlock()
				frameCopy := frame // to avoid race
				time.AfterFunc(sf.retransTimer(), func() {
					if sf.isPendingAck(frameCopy.fn) {
						sf.mpc.retransmit(frameCopy)
					}
				})
			}
		}
	}
}

func (sf *subflow) ack(fn uint64) {
	frame := composeFrame(fn, nil)
	_, err := sf.conn.Write(frame.buf)
	if err != nil {
		sf.close()
	}
	pool.Put(frame.buf)
}

func (sf *subflow) gotACK(fn uint64) {
	if fn == frameTypePing {
		log.Tracef("pong to %v", sf.to)
		sf.ack(frameTypePong)
		return
	}
	if fn == frameTypePong {
		if probeStart := sf.probeStart.Load(); probeStart != nil {
			start := probeStart.(time.Time)
			if !start.IsZero() {
				rtt := time.Since(start)
				sf.tracker.updateRTT(rtt)
				sf.emaRTT.UpdateDuration(rtt)
				sf.probeStart.Store(time.Time{})
				return
			}
		}
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
		sf.tracker.updateRTT(rtt)
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
	log.Tracef("ping %v", sf.to)
	sf.ack(frameTypePing)
	sf.probeStart.Store(time.Now())
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
		log.Tracef("closing subflow to %v", sf.to)
		sf.mpc.remove(sf)
		sf.conn.Close()
		close(sf.chClose)
	})
}
