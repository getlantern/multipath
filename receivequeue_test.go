package bond

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRead(t *testing.T) {
	q := newReceiveQueue(2)
	fn := uint64(0)
	addFrame := func(s string) {
		fn++
		q.add(frame{fn: fn, bytes: []byte(s)})
	}
	b := make([]byte, 3)
	shouldRead := func(s string) {
		n, err := q.read(b)
		assert.NoError(t, err)
		assert.Equal(t, s, string(b[:n]))
	}

	addFrame("abcd")
	shouldRead("abc")
	addFrame("abcd")
	shouldRead("dab")
	shouldRead("cd")
	addFrame("abcd")
	// adding the same frame number again should have no effect
	q.add(frame{fn: fn, bytes: []byte("1234")})
	shouldRead("abc")
	shouldRead("d")

	shouldWaitBeforeRead := func(d time.Duration, s string) {
		start := time.Now()
		n, err := q.read(b)
		assert.NoError(t, err)
		assert.Equal(t, s, string(b[:n]))
		assert.InDelta(t, time.Since(start), d, float64(50*time.Millisecond))
	}
	delay := 100 * time.Millisecond
	time.AfterFunc(delay, func() {
		addFrame("abcd")
	})
	shouldWaitBeforeRead(delay, "abc")
	time.AfterFunc(delay, func() {
		addFrame("abc")
	})
	shouldWaitBeforeRead(0, "d")
	shouldWaitBeforeRead(delay, "abc")

	// frames can be added out of order
	q.add(frame{fn: fn + 2, bytes: []byte("1234")})
	time.AfterFunc(delay, func() {
		addFrame("abcd")
	})
	shouldWaitBeforeRead(delay, "abc")
	shouldWaitBeforeRead(0, "d12")

}
