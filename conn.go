package multipath

import (
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type mpConn struct {
	cid     uint64
	nextFN     uint64
	subflows   []*subflow
	muSubflows sync.RWMutex
	recvQueue  *receiveQueue
}

func newMPConn(cid uint64) *mpConn {
	return &mpConn{cid: cid,
		nextFN:    minFrameNumber - 1,
		recvQueue: newReceiveQueue(4096),
	}
}
func (bc *mpConn) Read(b []byte) (n int, err error) {
	return bc.recvQueue.read(b)
}

func (bc *mpConn) Write(b []byte) (n int, err error) {
	bc.first().sendQueue <- composeFrame(atomic.AddUint64(&bc.nextFN, 1), b)
	return len(b), nil
}

func (bc *mpConn) Close() error {
	bc.muSubflows.RLock()
	defer bc.muSubflows.RUnlock()
	for _, sf := range bc.subflows {
		sf.close()
	}
	return nil
}

func (bc *mpConn) LocalAddr() net.Addr {
	panic("not implemented")
}

func (bc *mpConn) RemoteAddr() net.Addr {
	panic("not implemented")
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
		}
	}
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

func (bc *mpConn) add(c net.Conn, clientSide bool, probeStart time.Time) {
	bc.muSubflows.Lock()
	defer bc.muSubflows.Unlock()
	bc.subflows = append(bc.subflows, startSubflow(c, bc, clientSide, probeStart))
}

func (bc *mpConn) remove(theSubflow *subflow) {
	bc.muSubflows.Lock()
	defer bc.muSubflows.Unlock()
	var remains []*subflow
	for _, sf := range bc.subflows {
		if sf != theSubflow {
			remains = append(remains, sf)
		}
	}
	bc.subflows = remains
}
