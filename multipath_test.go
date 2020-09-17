package multipath

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestE2E(t *testing.T) {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
	listeners := []net.Listener{}
	trackers := []StatsTracker{}
	dialers := []Dialer{}
	for i := 0; i < 3; i++ {
		l, err := net.Listen("tcp", ":")
		if !assert.NoError(t, err) {
			continue
		}
		var lock sync.Mutex
		listeners = append(listeners, &testListener{l, delayEnforcer{cond: sync.NewCond(&lock)}, l})
		trackers = append(trackers, NullTracker{})
		// simulate one or more dialers to each listener
		for j := 0; j <= rand.Intn(5); j++ {
			var lock sync.Mutex
			idx := len(dialers)
			dialers = append(dialers, &testDialer{delayEnforcer{cond: sync.NewCond(&lock)}, l.Addr().String(), idx})
		}
	}
	log.Debugf("Testing with %d listeners and %d dialers", len(listeners), len(dialers))
	bl := NewListener(listeners, trackers)
	bd := NewDialer("endpoint", dialers)

	go func() {
		for {
			conn, err := bl.Accept()
			assert.NoError(t, err)
			go func() {
				b := make([]byte, 4)
				for {
					_, err := io.ReadFull(conn, b)
					assert.NoError(t, err)
					log.Debugf("server read '%v'", string(b))
					n, err := conn.Write(b)
					assert.NoError(t, err)
					assert.Equal(t, 4, n)
					log.Debugf("server written back '%v'", string(b))
				}
			}()
		}
	}()
	conn, err := bd.DialContext(context.Background())
	if !assert.NoError(t, err) {
		return
	}
	b := make([]byte, 4)
	roundtrip := func() {
		for i := 0; i < 10; i++ {
			copy(b, []byte(strconv.Itoa(i)))
			n, err := conn.Write(b)
			assert.NoError(t, err)
			assert.Equal(t, len(b), n)
			log.Debugf("client written '%v'", string(b))
			_, err = io.ReadFull(conn, b)
			assert.NoError(t, err)
			log.Debugf("client read '%v'", string(b))
		}
	}
	roundtrip()

	for i := 0; i < len(listeners)-1; i++ {
		log.Debugf("========listener[%d] is hanging", i)
		listeners[i].(*testListener).setDelay(time.Hour)
		roundtrip()
	}
	for i := 0; i < len(dialers)-1; i++ {
		log.Debugf("========dialer[%d] is hanging", i)
		dialers[i].(*testDialer).setDelay(time.Hour)
		roundtrip()
	}
	log.Debug("========reenabled the first pair")
	dialers[0].(*testDialer).setDelay(0)
	listeners[0].(*testListener).setDelay(0)
	log.Debug("========the last listener is hanging")
	listeners[len(listeners)-1].(*testListener).setDelay(time.Hour)
	roundtrip()
	dialers[len(dialers)-1].(*testDialer).setDelay(time.Hour)
	log.Debug("========the last dialer is hanging")
	roundtrip()
}

type testDialer struct {
	delayEnforcer
	addr string
	idx  int
}

func (td *testDialer) DialContext(ctx context.Context) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", td.addr)
	if err != nil {
		return nil, err
	}
	return &laggedConn{conn, conn, td.delayEnforcer.sleep}, nil
}

func (td *testDialer) Label() string {
	return fmt.Sprintf("test dialer #%d to %v", td.idx, td.addr)
}

type testListener struct {
	net.Listener
	delayEnforcer
	l net.Listener
}

func (tl *testListener) Accept() (net.Conn, error) {
	conn, err := tl.l.Accept()
	if err != nil {
		return nil, err
	}
	return &laggedConn{conn, conn, tl.delayEnforcer.sleep}, nil
}

type laggedConn struct {
	net.Conn
	conn  net.Conn // has to be the same as the net.Conn
	sleep func()
}

func (c *laggedConn) Read(b []byte) (int, error) {
	c.sleep()
	return c.conn.Read(b)
}

func TestDelayEnforcer(t *testing.T) {
	var lock sync.Mutex
	d := delayEnforcer{cond: sync.NewCond(&lock)}
	var wg sync.WaitGroup
	d.setDelay(time.Hour)
	wg.Add(1)
	start := time.Now()
	go func() {
		d.sleep()
		wg.Done()
	}()
	time.Sleep(100 * time.Millisecond)
	d.setDelay(0)
	wg.Wait()
	assert.InDelta(t, time.Since(start), 100*time.Millisecond, float64(10*time.Millisecond))
}

type delayEnforcer struct {
	delay int64
	cond  *sync.Cond
}

func (e *delayEnforcer) setDelay(d time.Duration) {
	atomic.StoreInt64(&e.delay, int64(d))
	e.cond.Broadcast()
}

func (e *delayEnforcer) sleep() {
	e.cond.L.Lock()
	defer e.cond.L.Unlock()
	for {
		d := atomic.LoadInt64(&e.delay)
		if delay := time.Duration(d); delay > 0 {
			log.Debugf("sleep for %v", delay)
			time.AfterFunc(delay, func() {
				e.cond.Broadcast()
			})
			e.cond.Wait()
		} else {
			return
		}
	}
}
