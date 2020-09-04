package multipath

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	pool "github.com/libp2p/go-buffer-pool"
)

// receiveQueue keeps received frames for the upper layer to read. It is
// maintained as a ring buffer with fixed size. It takes advantage of the fact
// that the frame number is consequential, so when a new frame arrives, it is
// placed at the position indexed by the remainder of the frame number divided
// by the buffer size.
type receiveQueue struct {
	buf  []frame
	size uint64
	// rp stands for read pointer, point to the index of the frame containing
	// data yet to be read.
	rp           uint64
	cond         *sync.Cond
	condAdd      *sync.Cond
	readDeadline time.Time
	closed       uint32 // 1 == true, 0 == false
}

func newReceiveQueue(size int) *receiveQueue {
	rq := &receiveQueue{
		buf:     make([]frame, size),
		size:    uint64(size),
		rp:      minFrameNumber % uint64(size), // frame number starts with minFrameNumber, so should the read pointer
		cond:    sync.NewCond(&sync.Mutex{}),
		condAdd: sync.NewCond(&sync.Mutex{}),
	}
	return rq
}

func (rq *receiveQueue) add(f *frame) {
	for {
		if rq.tryAdd(f) {
			return
		}
		log.Error("would override unconsumed frame! wait for available slot")
		if !rq.waitForSlot() {
			pool.Put(f.bytes)
			return
		}
	}
}

func (rq *receiveQueue) tryAdd(f *frame) bool {
	idx := f.fn % rq.size
	rq.cond.L.Lock()
	defer rq.cond.L.Unlock()
	if rq.buf[idx].bytes == nil {
		// empty slot
		rq.buf[idx] = *f
		if idx == rq.rp {
			rq.cond.Signal()
		}
		return true
	} else if rq.buf[idx].fn == f.fn {
		// retransmission, ignore
		pool.Put(f.bytes)
		return true
	}
	return false
}

func (rq *receiveQueue) waitForSlot() bool {
	rq.condAdd.L.Lock()
	rq.condAdd.Wait()
	rq.condAdd.L.Unlock()
	if atomic.LoadUint32(&rq.closed) == 1 {
		return false
	}
	return true
}

func (rq *receiveQueue) read(b []byte) (int, error) {
	rq.cond.L.Lock()
	defer rq.cond.L.Unlock()
	for {
		if rq.buf[rq.rp].bytes != nil {
			break
		}
		if atomic.LoadUint32(&rq.closed) == 1 {
			return 0, ErrClosed
		}
		if rq.dlExceeded() {
			return 0, context.DeadlineExceeded
		}
		rq.cond.Wait()
	}
	totalN := 0
	for {
		if rq.buf[rq.rp].bytes == nil {
			if totalN > 0 {
				rq.condAdd.Signal()
				return totalN, nil
			}
			panic("should not happen")
		}
		cur := rq.buf[rq.rp].bytes
		n := copy(b[totalN:], cur)
		if n == len(cur) {
			pool.Put(rq.buf[rq.rp].bytes)
			rq.buf[rq.rp].bytes = nil
			rq.rp = (rq.rp + 1) % rq.size
		} else {
			// The frames in the ring buffer are never overridden, so we can
			// safely update the bytes to reflect the next read position.
			rq.buf[rq.rp].bytes = cur[n:]
		}
		totalN += n
		if totalN == len(b) {
			rq.condAdd.Signal()
			return totalN, nil
		}
	}
}

func (rq *receiveQueue) setReadDeadline(dl time.Time) {
	rq.cond.L.Lock()
	rq.readDeadline = dl
	rq.cond.L.Unlock()
	if !dl.IsZero() {
		ttl := dl.Sub(time.Now())
		if ttl <= 0 {
			rq.cond.Broadcast()
		} else {
			time.AfterFunc(ttl, rq.cond.Broadcast)
		}
	}
}

func (rq *receiveQueue) dlExceeded() bool {
	return !rq.readDeadline.IsZero() && !rq.readDeadline.After(time.Now())
}

func (rq *receiveQueue) close() {
	atomic.StoreUint32(&rq.closed, 1)
	rq.cond.Broadcast()
	rq.condAdd.Broadcast()
}
