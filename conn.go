package bond

import (
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type bondConn struct {
	bondID     uint64
	nextFN     uint64
	subflows   []*subflow
	muSubflows sync.RWMutex
	recvQueue  *receiveQueue
}

func newBondConn(bondID uint64) *bondConn {
	return &bondConn{bondID: bondID,
		nextFN:    minFrameNumber - 1,
		recvQueue: newReceiveQueue(4096),
	}
}
func (bc *bondConn) Read(b []byte) (n int, err error) {
	return bc.recvQueue.read(b)
}

func (bc *bondConn) Write(b []byte) (n int, err error) {
	bc.first().sendQueue <- composeFrame(atomic.AddUint64(&bc.nextFN, 1), b)
	return len(b), nil
}

func (bc *bondConn) Close() error {
	bc.muSubflows.RLock()
	defer bc.muSubflows.RUnlock()
	for _, sf := range bc.subflows {
		sf.close()
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
	bc.muSubflows.RLock()
	defer bc.muSubflows.RUnlock()
	for _, sf := range bc.subflows {
		if err := sf.conn.SetWriteDeadline(t); err != nil {
			return err
		}
	}
	return nil
}

func (bc *bondConn) first() *subflow {
	bc.muSubflows.RLock()
	defer bc.muSubflows.RUnlock()
	return bc.subflows[0]
}

func (bc *bondConn) retransmit(frame sendFrame) {
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

func (bc *bondConn) sortSubflows() []*subflow {
	bc.muSubflows.Lock()
	defer bc.muSubflows.Unlock()
	sort.Slice(bc.subflows, func(i, j int) bool {
		return bc.subflows[i].emaRTT.GetDuration() > bc.subflows[j].emaRTT.GetDuration()
	})
	subflowsCopy := make([]*subflow, len(bc.subflows))
	copy(subflowsCopy, bc.subflows)
	return subflowsCopy
}

func (bc *bondConn) add(c net.Conn, clientSide bool, probeStart time.Time) {
	bc.muSubflows.Lock()
	defer bc.muSubflows.Unlock()
	bc.subflows = append(bc.subflows, startSubflow(c, bc, clientSide, probeStart))
}

func (bc *bondConn) remove(theSubflow *subflow) {
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
