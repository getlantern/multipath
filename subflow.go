package bond

import (
	"bufio"
	"bytes"
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

	sendQueue    chan frame
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
		sendQueue:  make(chan frame),
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
		// probe immediately so the server can calculate the RTT by taking the
		// time between echoing lead bytes and receiving probe
		sf.probe()
	}
	for {
		sz, err := ReadVarInt(r)
		if err != nil {
			return err
		}
		fn, err := ReadVarInt(r)
		if err != nil {
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
			return err
		}
		sf.ack(fn)
		sf.bc.recvQueue.add(frame{fn: fn, bytes: buf})
	}
}

func (sf *subflow) writeFrame(frame *frame) (done bool, isDataFrame bool) {
	sz := len(frame.bytes)
	buf := pool.Get(8 + 8 + sz)
	defer pool.Put(buf)
	b := bytes.NewBuffer(buf[:0])
	WriteVarInt(b, uint64(sz))
	WriteVarInt(b, frame.fn)
	isDataFrame = sz > 0
	if isDataFrame {
		b.Write(frame.bytes)
	}
	length := b.Len()
	if _, err := sf.conn.Write(buf[:length]); err != nil {
		return false, false
	}
	log.Tracef("Done writing %v bytes to wire", length)
	return true, isDataFrame
}

func (sf *subflow) sendLoop() {
	for frame := range sf.sendQueue {
		written, isDataFrame := sf.writeFrame(&frame)
		if !written {
			sf.bc.retransmit(&frame)
			continue
		}
		if isDataFrame {
			fn := frame.fn
			now := time.Now()
			sf.muPendingAck.Lock()
			sf.pendingAck[fn] = now
			sf.muPendingAck.Unlock()
			if frame.firstSentAt.IsZero() {
				frame.firstSentAt = now
			}
			if time.Since(frame.firstSentAt) < time.Minute {
				frameCopy := frame
				time.AfterFunc(sf.retransTimer(), func() {
					if !sf.isAcked(fn) {
						sf.bc.retransmit(&frameCopy)
					}
				})
			}
		}
	}
}

func (sf *subflow) ack(fn uint64) {
	log.Tracef("acking frame# %v", fn)
	_, _ = sf.writeFrame(&frame{fn: fn, bytes: nil})
}

func (sf *subflow) gotACK(fn uint64) {
	if fn == 0 && !sf.probeStart.IsZero() {
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

func (sf *subflow) isAcked(fn uint64) bool {
	sf.muPendingAck.Lock()
	defer sf.muPendingAck.Unlock()
	_, found := sf.pendingAck[fn]
	return !found
}

func (sf *subflow) probe() {
	_, _ = sf.writeFrame(&frame{fn: 0, bytes: nil})
	sf.probeStart = time.Now()
}

func (sf *subflow) retransTimer() time.Duration {
	d := sf.emaRTT.GetDuration() * 2
	if d < 100*time.Millisecond {
		d = 100 * time.Millisecond
	}
	return d
}
