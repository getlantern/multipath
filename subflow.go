package bond

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	"github.com/getlantern/ema"
	pool "github.com/libp2p/go-buffer-pool"
)

type subflow struct {
	conn net.Conn
	bc   *bondConn

	chClose      chan struct{}
	closeOnce    sync.Once
	sendQueue    chan sendFrame
	pendingAck   map[uint64]time.Time
	muPendingAck sync.Mutex
	probeStart   time.Time
	probeTimer   *time.Timer
	emaRTT       *ema.EMA
}

func startSubflow(c net.Conn, bc *bondConn, clientSide bool) *subflow {
	sf := &subflow{
		conn:       c,
		bc:         bc,
		chClose:    make(chan struct{}),
		sendQueue:  make(chan sendFrame),
		pendingAck: make(map[uint64]time.Time),
		probeStart: time.Now(),
		probeTimer: time.NewTimer(0),
		emaRTT:     ema.NewDuration(time.Second, 0.1)}
	go func() {
		log.Error(sf.readLoop(clientSide))
	}()
	go sf.sendLoop()
	return sf
}

func (sf *subflow) readLoop(expectLeadBytes bool) error {
	r := bufio.NewReader(sf.conn)
	if expectLeadBytes {
		var leadBytes [1 + 8]byte
		_, err := io.ReadFull(r, leadBytes[:])
		if err != nil {
			return err
		}
		version := uint8(leadBytes[0])
		if uint8(version) != 0 {
			return ErrUnexpectedVersion
		}
		bondID := binary.LittleEndian.Uint64(leadBytes[1:])
		if bondID != sf.bc.bondID {
			return ErrUnexpectedBondID
		}
		sf.emaRTT.UpdateDuration(time.Since(sf.probeStart))
		// pong immediately so the server can calculate the RTT by taking the
		// time between echoing lead bytes and receiving the pong frame.
		sf.ack(frameTypePong)
	}
	for {
		sz, err := ReadVarInt(r)
		if err != nil {
			sf.closeSelf()
			return err
		}
		fn, err := ReadVarInt(r)
		if err != nil {
			sf.closeSelf()
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
			sf.closeSelf()
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
			sf.closeSelf()
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
		sf.closeSelf()
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
	sendTime, found := sf.pendingAck[fn]
	if found {
		// it's okay to calculate RTT this way because ack frame is always sent
		// back through the same subflow.
		sf.emaRTT.UpdateDuration(time.Since(sendTime))
		delete(sf.pendingAck, fn)
	}
	sf.muPendingAck.Unlock()
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

func (sf *subflow) closeSelf() {
	sf.closeOnce.Do(func() {
		sf.bc.remove(sf)
		sf.conn.Close()
		close(sf.sendQueue)
		close(sf.chClose)
	})
}
