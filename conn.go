package multipath

import (
	"bytes"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pool "github.com/libp2p/go-buffer-pool"
)

type mpConn struct {
	cid        uint64
	nextFN     uint64
	subflows   []*subflow
	muSubflows sync.RWMutex
	recvQueue  *receiveQueue
	closed     uint32 // 1 == true, 0 == false
}

func newMPConn(cid uint64) *mpConn {
	return &mpConn{cid: cid,
		nextFN:    minFrameNumber - 1,
		recvQueue: newReceiveQueue(recieveQueueLength),
	}
}
func (bc *mpConn) Read(b []byte) (n int, err error) {
	return bc.recvQueue.read(b)
}

func composeFrame(fn uint64, b []byte) sendFrame {
	sz := len(b)
	buf := pool.Get(8 + 8 + sz)
	wb := bytes.NewBuffer(buf[:0])
	WriteVarInt(wb, uint64(sz))
	WriteVarInt(wb, fn)
	if sz > 0 {
		wb.Write(b)
	}
	return sendFrame{fn: fn, sz: uint64(sz), buf: wb.Bytes()}
}

func (bc *mpConn) Write(b []byte) (n int, err error) {
	if atomic.LoadUint32(&bc.closed) == 1 {
		return 0, ErrClosed
	}
	bc.first().sendQueue <- composeFrame(atomic.AddUint64(&bc.nextFN, 1), b)
	return len(b), nil
}

func (bc *mpConn) Close() error {
	atomic.StoreUint32(&bc.closed, 1)
	bc.recvQueue.close()
	bc.muSubflows.RLock()
	defer bc.muSubflows.RUnlock()
	for _, sf := range bc.subflows {
		sf.close()
	}
	return nil
}

type fakeAddr struct{}

func (fakeAddr) Network() string { return "multipath" }
func (fakeAddr) String() string  { return "multipath" }

func (bc *mpConn) LocalAddr() net.Addr {
	return fakeAddr{}
}

func (bc *mpConn) RemoteAddr() net.Addr {
	return fakeAddr{}
}

func (bc *mpConn) SetDeadline(t time.Time) error {
	bc.SetReadDeadline(t)
	return bc.SetWriteDeadline(t)
}

func (bc *mpConn) SetReadDeadline(t time.Time) error {
	bc.recvQueue.setReadDeadline(t)
	return nil
}

func (bc *mpConn) SetWriteDeadline(t time.Time) error {
	bc.muSubflows.RLock()
	defer bc.muSubflows.RUnlock()
	for _, sf := range bc.subflows {
		if err := sf.conn.SetWriteDeadline(t); err != nil {
			return err
		}
	}
	return nil
}

func (bc *mpConn) first() *subflow {
	bc.muSubflows.RLock()
	defer bc.muSubflows.RUnlock()
	return bc.subflows[0]
}

func (bc *mpConn) retransmit(frame sendFrame) {
	subflows := bc.sortSubflows()
	frame.retransmissions++
	if frame.retransmissions >= len(subflows)-1 {
		log.Debugf("Give up retransmitting frame# %v", frame.fn)
		return
	}
	for _, sf := range subflows {
		// choose the first subflow not waiting ack for this frame
		if !sf.isPendingAck(frame.fn) {
			log.Tracef("Retransmitting frame# %v", frame.fn)
			sf.sendQueue <- frame
			return
		}
	}
	log.Debug("No eligible subflow for retransmitting, skipped")
}

func (bc *mpConn) sortSubflows() []*subflow {
	bc.muSubflows.Lock()
	defer bc.muSubflows.Unlock()
	sort.Slice(bc.subflows, func(i, j int) bool {
		return bc.subflows[i].emaRTT.GetDuration() > bc.subflows[j].emaRTT.GetDuration()
	})
	subflowsCopy := make([]*subflow, len(bc.subflows))
	copy(subflowsCopy, bc.subflows)
	return subflowsCopy
}

func (bc *mpConn) add(c net.Conn, clientSide bool, probeStart time.Time, tracker statsTracker) {
	bc.muSubflows.Lock()
	defer bc.muSubflows.Unlock()
	bc.subflows = append(bc.subflows, startSubflow(c, bc, clientSide, probeStart, tracker))
}

func (bc *mpConn) remove(theSubflow *subflow) {
	bc.muSubflows.Lock()
	var remains []*subflow
	for _, sf := range bc.subflows {
		if sf != theSubflow {
			remains = append(remains, sf)
		}
	}
	bc.subflows = remains
	left := len(remains)
	bc.muSubflows.Unlock()
	if left == 0 {
		bc.Close()
	}
}
