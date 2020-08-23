package multipath

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestE2E(t *testing.T) {
	listeners := []net.Listener{}
	dialers := []Dialer{}
	for i := 0; i < 3; i++ {
		l, _ := net.Listen("tcp", ":")
		listeners = append(listeners, &testListener{l, l, 0})
		for j := 0; j < rand.Intn(5); j++ {
			dialers = append(dialers, &testDialer{0, "tcp", l.Addr().String()})
		}
	}
	bl := MPListener(listeners...)
	bd := MPDialer(dialers...)

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
	conn, err := bd.DialContext(context.Background(), "whatever", "whatever")
	assert.NoError(t, err)
	b := make([]byte, 4)
	roundtripTest := func() {
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
	roundtripTest()

	listeners[0].(*testListener).setDelay(time.Hour)
	roundtripTest()
	dialers[0].(*testDialer).setDelay(time.Hour)
	roundtripTest()
}

type testDialer struct {
	delay         int64
	network, addr string
}

func (td *testDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, td.network, td.addr)
	if err != nil {
		return nil, err
	}
	return &laggedConn{conn, conn, func() time.Duration { return time.Duration(atomic.LoadInt64(&td.delay)) }}, nil
}

func (td *testDialer) Label() string {
	return fmt.Sprintf("test dialer to %v %v", td.network, td.addr)
}

func (td *testDialer) setDelay(d time.Duration) {
	atomic.StoreInt64(&td.delay, int64(d))
}

type testListener struct {
	net.Listener
	l     net.Listener
	delay int64
}

func (tl *testListener) Accept() (net.Conn, error) {
	conn, err := tl.l.Accept()
	if err != nil {
		return nil, err
	}
	return &laggedConn{conn, conn, func() time.Duration { return time.Duration(atomic.LoadInt64(&tl.delay)) }}, nil
}

func (tl *testListener) setDelay(d time.Duration) {
	atomic.StoreInt64(&tl.delay, int64(d))
}

type laggedConn struct {
	net.Conn
	conn  net.Conn // has to be the same as the net.Conn
	delay func() time.Duration
}

func (c *laggedConn) Read(b []byte) (int, error) {
	delay := c.delay()
	if delay > 0 {
		log.Debugf("sleep for %v", delay)
		time.Sleep(delay)
	}
	return c.conn.Read(b)
}
