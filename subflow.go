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

type subflow struct {
	to   string
	conn net.Conn
	bc   *mpConn

	chClose      chan struct{}
	closeOnce    sync.Once
	sendQueue    chan sendFrame
	pendingAck   map[uint64]time.Time
	muPendingAck sync.Mutex
	probeStart   time.Time
	probeTimer   *time.Timer
	emaRTT       *ema.EMA
}

func startSubflow(c net.Conn, bc *mpConn, clientSide bool, probeStart time.Time) *subflow {
	sf := &subflow{
		to:         c.RemoteAddr().String(),
		conn:       c,
		bc:         bc,
		chClose:    make(chan struct{}),
		sendQueue:  make(chan sendFrame),
		pendingAck: make(map[uint64]time.Time),
		probeTimer: time.NewTimer(0),
		emaRTT:     ema.NewDuration(time.Second, 0.1)}
	if clientSide {
		sf.emaRTT.SetDuration(time.Since(probeStart))
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

func (sf *subflow) readLoop() error {
	r := bufio.NewReader(sf.conn)
	for {
		sz, err := ReadVarInt(r)
		if err != nil {
			sf.close()
			return err
		}
		fn, err := ReadVarInt(r)
		if err != nil {
			sf.close()
			return err
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
			return err
		}
		sf.ack(fn)
		sf.bc.recvQueue.add(frame{fn: fn, bytes: buf})
		select {
		case <-sf.chClose:
			return nil
		default:
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
			sf.muPendingAck.Lock()
			sf.pendingAck[frame.fn] = now
			sf.muPendingAck.Unlock()
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
		sf.emaRTT.UpdateDuration(time.Since(sf.probeStart))
		sf.probeStart = time.Time{}
		return
	}
	log.Tracef("Got ack for frame# %v", fn)
	sf.muPendingAck.Lock()
	defer sf.muPendingAck.Unlock()
	sendTime, found := sf.pendingAck[fn]
	if found {
		// it's okay to calculate RTT this way because ack frame is always sent
		// back through the same subflow, and a data frame is never sent over
		// the same subflow more than once.
		sf.emaRTT.UpdateDuration(time.Since(sendTime))
		delete(sf.pendingAck, fn)
	}
}

func (sf *subflow) isPendingAck(fn uint64) bool {
	sf.muPendingAck.Lock()
	defer sf.muPendingAck.Unlock()
	_, found := sf.pendingAck[fn]
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
