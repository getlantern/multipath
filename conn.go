package bond

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getlantern/ema"
	pool "github.com/libp2p/go-buffer-pool"
)

type bondConn struct {
	bondID          uint64
	nextFN          uint64
	conns           []net.Conn
	recvQueue       *receiveQueue
	sendQueue       chan frame
	pendingAck      map[uint64]time.Time
	muPendingAck    sync.Mutex
	emaRTT          *ema.EMA
	leadBytesSentAt time.Time
}

func newBondConn(bondID uint64) *bondConn {
	return &bondConn{bondID: bondID,
		recvQueue:  newReceiveQueue(4096),
		sendQueue:  make(chan frame),
		pendingAck: make(map[uint64]time.Time),
		emaRTT:     ema.NewDuration(time.Second, 0.1),
	}
}
func (bc *bondConn) Read(b []byte) (n int, err error) {
	return bc.recvQueue.read(b)
}

func (bc *bondConn) Write(b []byte) (n int, err error) {
	bc.sendQueue <- frame{fn: atomic.AddUint64(&bc.nextFN, 1), bytes: b}
	return len(b), nil
}

func (bc *bondConn) Close() error {
	for _, c := range bc.conns {
		c.Close()
	}
	return nil
}

func (bc *bondConn) LocalAddr() net.Addr {
	panic("not implemented")
}

func (bc *bondConn) RemoteAddr() net.Addr {
	panic("not implemented")
}

func (bc *bondConn) SetDeadline(t time.Time) error {
	bc.SetReadDeadline(t)
	return bc.SetWriteDeadline(t)
}

func (bc *bondConn) SetReadDeadline(t time.Time) error {
	bc.recvQueue.setReadDeadline(t)
	return nil
}

func (bc *bondConn) SetWriteDeadline(t time.Time) error {
	for _, c := range bc.conns {
		if err := c.SetWriteDeadline(t); err != nil {
			return err
		}
	}
	return nil
}

func (bc *bondConn) addClientConn(c net.Conn) error {
	var leadBytes [1 + 8]byte
	// version is implicitly set to 0
	binary.LittleEndian.PutUint64(leadBytes[1:], bc.bondID)
	if _, err := c.Write(leadBytes[:]); err != nil {
		return err
	}
	bc.leadBytesSentAt = time.Now()
	bc.conns = append(bc.conns, c)
	go func() {
		log.Error(bc.readFrom(bufio.NewReader(c), true))
	}()
	go bc.sendLoop()
	return nil
}

func (bc *bondConn) add(c net.Conn) {
	bc.conns = append(bc.conns, c)
	go func() {
		log.Error(bc.readFrom(bufio.NewReader(c), false))
	}()
	go bc.sendLoop()
}

func (bc *bondConn) readFrom(r *bufio.Reader, expectLeadBytes bool) error {
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
		if bondID != bc.bondID {
			return ErrUnexpectedBondID
		}
		bc.emaRTT.UpdateDuration(time.Since(bc.leadBytesSentAt))
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
			bc.acked(fn)
			continue
		}
		buf := pool.Get(int(sz))
		_, err = io.ReadFull(r, buf)
		if err != nil {
			pool.Put(buf)
			return err
		}
		bc.ack(fn)
		bc.recvQueue.add(frame{fn: fn, bytes: buf})
	}
}

func (bc *bondConn) sendLoop() {
	for frame := range bc.sendQueue {
		sz := len(frame.bytes)
		buf := pool.Get(8 + 8 + sz)
		b := bytes.NewBuffer(buf[:0])
		fn := frame.fn
		WriteVarInt(b, uint64(sz))
		WriteVarInt(b, fn)
		isDataFrame := sz > 0
		if isDataFrame {
			b.Write(frame.bytes)
		}
		bc.writeToWire(b.Bytes())
		if isDataFrame {
			bc.muPendingAck.Lock()
			bc.pendingAck[fn] = time.Now()
			bc.muPendingAck.Unlock()
			if frame.firstSentAt.IsZero() {
				frame.firstSentAt = time.Now()
			}
			if time.Since(frame.firstSentAt) < time.Minute {
				time.AfterFunc(bc.retransTimer(), func() {
					if !bc.isAcked(fn) {
						log.Tracef("Resending frame# %v: %v", fn, frame.bytes)
						bc.sendQueue <- frame
					}
				})
			}
		}
		pool.Put(buf)
	}
}

func (bc *bondConn) retransTimer() time.Duration {
	d := bc.emaRTT.GetDuration() * 2
	if d < 100*time.Millisecond {
		d = 100 * time.Millisecond
	}
	return d
}

func (bc *bondConn) writeToWire(b []byte) {
	for {
		if _, err := bc.nextConnToSend().Write(b); err == nil {
			log.Tracef("Done writing %v bytes to wire", len(b))
			break
		}
	}
}

func (bc *bondConn) nextConnToSend() net.Conn {
	return bc.conns[0]
}

func (bc *bondConn) ack(fn uint64) {
	log.Tracef("acking frame# %v", fn)
	bc.sendQueue <- frame{fn: fn, bytes: nil}
}

func (bc *bondConn) acked(fn uint64) {
	log.Tracef("Got ack for frame# %v", fn)
	bc.muPendingAck.Lock()
	sendTime, found := bc.pendingAck[fn]
	if found {
		bc.emaRTT.UpdateDuration(time.Since(sendTime))
		delete(bc.pendingAck, fn)
	}
	bc.muPendingAck.Unlock()
}

func (bc *bondConn) isAcked(fn uint64) bool {
	bc.muPendingAck.Lock()
	defer bc.muPendingAck.Unlock()
	_, found := bc.pendingAck[fn]
	return !found
}
