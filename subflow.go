package multipath

import (
	"bufio"
	"io"
	"math/rand"
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
	sendQueue     chan *sendFrame
	pendingAcks   map[uint64]pendingAck
	muPendingAcks sync.Mutex
	probeStart    atomic.Value // time.Time
	emaRTT        *ema.EMA
	tracker       StatsTracker
}

func startSubflow(to string, c net.Conn, mpc *mpConn, clientSide bool, probeStart time.Time, tracker StatsTracker) *subflow {
	sf := &subflow{
		to:          to,
		conn:        c,
		mpc:         mpc,
		chClose:     make(chan struct{}),
		sendQueue:   make(chan *sendFrame),
		pendingAcks: make(map[uint64]pendingAck),
		emaRTT:      ema.NewDuration(longRTT, rttAlpha),
		tracker:     tracker,
	}
	go sf.sendLoop()
	if clientSide {
		initialRTT := time.Since(probeStart)
		tracker.UpdateRTT(initialRTT)
		sf.emaRTT.SetDuration(initialRTT)
		// pong immediately so the server can calculate the RTT between when it
		// sends the leading bytes and receives the pong frame.
		sf.ack(frameTypePong)
	} else {
		sf.probeStart.Store(probeStart)
	}
	go func() {
		if err := sf.readLoop(); err != nil {
			log.Debugf("read loop to %s ended: %v", sf.to, err)
		}
	}()
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
			log.Tracef("got frame# %d from %s with %d bytes", fn, sf.to, sz)
			buf := pool.Get(int(sz))
			_, err = io.ReadFull(r, buf)
			if err != nil {
				pool.Put(buf)
				sf.close()
				return
			}
			sf.ack(fn)
			ch <- &frame{fn: fn, bytes: buf}
			sf.tracker.OnRecv(sz)
			select {
			case <-sf.chClose:
				return
			default:
				// continue
			}
		}
	}()
	probeTimer := time.NewTimer(randomize(probeInterval))
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
			probeTimer.Reset(randomize(probeInterval))
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
			_, err := sf.conn.Write(frame.buf)
			if err != nil {
				sf.close()
				if frame.isDataFrame() {
					sf.mpc.retransmit(frame)
					continue
				}
			}
			if !frame.isDataFrame() {
				frame.release()
				continue
			}
			if frame.retransmissions == 0 {
				sf.tracker.OnSent(frame.sz)
			} else {
				sf.tracker.OnRetransmit(frame.sz)
			}
			log.Tracef("done writing frame %d with %d bytes via %s", frame.fn, frame.sz, sf.to)
			sf.muPendingAcks.Lock()
			sf.pendingAcks[frame.fn] = pendingAck{int(frame.sz), time.Now()}
			sf.muPendingAcks.Unlock()
			frameCopy := *frame // to avoid race
			d := sf.retransTimer()
			time.AfterFunc(d, func() {
				if sf.isPendingAck(frameCopy.fn) {
					// No ack means the subflow fails or has a longer RTT
					sf.updateRTT(d)
					sf.mpc.retransmit(&frameCopy)
				} else {
					// It is ok to release buffer here as the frame will never
					// be retransmitted again.
					frame.release()
				}
			})
		}
	}
}

func (sf *subflow) ack(fn uint64) {
	sf.sendQueue <- composeFrame(fn, nil)
}

func (sf *subflow) gotACK(fn uint64) {
	if fn == frameTypePing {
		log.Tracef("pong to %s", sf.to)
		sf.ack(frameTypePong)
		return
	}
	if fn == frameTypePong {
		if probeStart := sf.probeStart.Load(); probeStart != nil {
			start := probeStart.(time.Time)
			if !start.IsZero() {
				sf.updateRTT(time.Since(start))
				sf.probeStart.Store(time.Time{})
				return
			}
		}
	}
	log.Tracef("got ack for frame# %d from %s", fn, sf.to)
	sf.muPendingAcks.Lock()
	defer sf.muPendingAcks.Unlock()
	pendingAck, found := sf.pendingAcks[fn]
	if found && pendingAck.sz < maxFrameSizeToCalculateRTT {
		// it's okay to calculate RTT this way because ack frame is always sent
		// back through the same subflow, and a data frame is never sent over
		// the same subflow more than once.
		sf.updateRTT(time.Since(pendingAck.sentAt))
		delete(sf.pendingAcks, fn)
	}
}

func (sf *subflow) updateRTT(rtt time.Duration) {
	log.Tracef("RTT of %s: %v", sf.to, rtt)
	sf.tracker.UpdateRTT(rtt)
	sf.emaRTT.UpdateDuration(rtt)
}

func (sf *subflow) isPendingAck(fn uint64) bool {
	sf.muPendingAcks.Lock()
	defer sf.muPendingAcks.Unlock()
	_, found := sf.pendingAcks[fn]
	return found
}

func (sf *subflow) probe() {
	log.Tracef("ping %s", sf.to)
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
		log.Tracef("closing subflow to %s", sf.to)
		sf.mpc.remove(sf)
		sf.conn.Close()
		close(sf.chClose)
	})
}

func randomize(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int63n(int64(d)))
}
