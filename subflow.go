package multipath

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/getlantern/ema"
	pool "github.com/libp2p/go-buffer-pool"
)

type pendingAck struct {
	fn         uint64
	sz         uint64
	sentAt     time.Time
	outboundSf *subflow
	framePtr   *sendFrame
}

type subflow struct {
	to   string
	conn net.Conn
	mpc  *mpConn

	chClose       chan struct{}
	closeOnce     sync.Once
	sendQueue     chan *sendFrame
	pendingPing   *pendingAck // Only for pings
	muPendingPing sync.RWMutex
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
		pendingPing: nil,
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
		// server side subflow expects a pong frame to calculate RTT.
		sf.muPendingPing.Lock()
		sf.pendingPing = &pendingAck{frameTypePong, 0, probeStart, sf, nil}
		sf.muPendingPing.Unlock()
	}
	go func() {
		if err := sf.readLoop(); err != nil && err != io.EOF {
			log.Debugf("read loop to %s ended: %v", sf.to, err)
		}
	}()
	return sf
}

func (sf *subflow) readLoop() (err error) {
	ch := make(chan *frame)
	r := byteReader{Reader: sf.conn}
	go func() {
		defer close(ch)
		for {
			var sz, fn uint64
			sz, err = ReadVarInt(r)
			if err != nil {
				sf.close()
				return
			}
			fn, err = ReadVarInt(r)
			if err != nil {
				sf.close()
				return
			}
			if sz == 0 {
				sf.gotACK(fn)
				continue
			}
			log.Tracef("got frame %d from %s with %d bytes", fn, sf.to, sz)
			if sz > 1<<20 {
				log.Errorf("Frame of size %v from %s is impossible", sz, sf.to)
				sf.close()
				return
			}
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
			sf.mpc.recvQueue.add(frame, sf)
			if !probeTimer.Stop() {
				<-probeTimer.C
			}
			probeTimer.Reset(randomize(probeInterval))
		case <-probeTimer.C:
			go sf.probe()
			probeTimer.Reset(randomize(probeInterval))
		}
	}
}

func (sf *subflow) sendLoop() {
	for {
		select {
		case <-sf.chClose:
			return
		case frame := <-sf.sendQueue:
			if *frame.released == 1 {
				log.Errorf("Tried to send a frame that has already been released! Frame Number: %v", frame.fn)
				continue
			}

			sf.addPendingAck(frame)
			n, err := sf.conn.Write(frame.buf)

			// only wake up one re-transmitter, to better control the possible hored of them
			select {
			case sf.mpc.tryRetransmit <- true:
			default:
			}

			if err != nil {
				log.Debugf("failed to write frame %d to %s: %v", frame.fn, sf.to, err)
				// TODO: For temporary errors, maybe send the subflow to the
				// back of the line instead of closing it.
				sf.close()
				if frame.isDataFrame() {
					go sf.mpc.retransmit(frame)
				}
				return
			}
			if n != len(frame.buf) {
				panic(fmt.Sprintf("expect to write %d bytes on %s, written %d", len(frame.buf), sf.to, n))
			}
			if !frame.isDataFrame() {
				frame.release()
				continue
			}
			log.Tracef("done writing frame %d with %d bytes via %s", frame.fn, frame.sz, sf.to)
			if frame.retransmissions == 0 {
				sf.tracker.OnSent(frame.sz)
			} else {
				sf.tracker.OnRetransmit(frame.sz)
			}
		}
	}
}

func (sf *subflow) ack(fn uint64) {
	select {
	case <-sf.chClose:
	case sf.sendQueue <- composeFrame(fn, nil):
	}
}

func (sf *subflow) gotACK(fn uint64) {
	log.Tracef("got ack for frame %d from %s", fn, sf.to)
	if fn == frameTypePing {
		log.Tracef("pong to %s", sf.to)
		sf.ack(frameTypePong)
		return
	}

	sf.mpc.pendingAckMu.RLock()
	pending := sf.mpc.pendingAckMap[fn]
	if sf.mpc.pendingAckMap[fn] != nil {
		sf.mpc.pendingAckMu.RUnlock()
		sf.mpc.pendingAckMu.Lock()
		delete(sf.mpc.pendingAckMap, fn)
		sf.mpc.pendingAckMu.Unlock()
	} else {
		sf.mpc.pendingAckMu.RUnlock()
		return
	}

	if time.Since(pending.sentAt) < time.Second {
		pending.outboundSf.updateRTT(time.Since(pending.sentAt))
	} else {
		pending.outboundSf.updateRTT(time.Second)
	}
}

func (sf *subflow) updateRTT(rtt time.Duration) {
	sf.tracker.UpdateRTT(rtt)
	sf.emaRTT.UpdateDuration(rtt)
}

func (sf *subflow) getRTT() time.Duration {
	recorded := sf.emaRTT.GetDuration()
	// RTT is updated only when ack is received or retransmission timer raises,
	// which can be stale when the subflow starts hanging. If that happens, the
	// time since the earliest yet-to-be-acknowledged frame being sent is more
	// up-to-date.
	var realtime time.Duration
	sf.muPendingPing.RLock()
	realtime = time.Since(sf.pendingPing.sentAt)
	sf.muPendingPing.RUnlock()
	if realtime > recorded {
		return realtime
	} else {
		return recorded
	}
}

func (sf *subflow) addPendingAck(frame *sendFrame) {
	switch frame.fn {
	case frameTypePing:
		// we expect pong for ping
		sf.muPendingPing.Lock()
		sf.pendingPing = &pendingAck{frameTypePong, 0, time.Now(), sf, nil}
		sf.muPendingPing.Unlock()
	case frameTypePong:
		// expect no response for pong
	default:
		if frame.isDataFrame() {
			sf.mpc.pendingAckMu.Lock()
			sf.mpc.pendingAckMap[frame.fn] = &pendingAck{frame.fn, frame.sz, time.Now(), sf, frame}
			sf.mpc.pendingAckMu.Unlock()
		}
	}
}

func (sf *subflow) isPendingAck(fn uint64) bool {
	if fn > minFrameNumber {
		sf.mpc.pendingAckMu.RLock()
		defer sf.mpc.pendingAckMu.RUnlock()
		return sf.mpc.pendingAckMap[fn] != nil
	}
	return false
}

func (sf *subflow) probe() {
	log.Tracef("ping %s", sf.to)
	sf.ack(frameTypePing)
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
