package bond

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
)

type Dialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
	Label() string
}

type dialer struct {
	dialers []Dialer
}

var nextBondID uint64

func BondDialer(dialers ...Dialer) Dialer {
	return &dialer{dialers}
}

// DialContext dials the addr using all dialers and returns a bond contains
// connections from whatever dialers available.
func (d *dialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	bc := newBondConn(atomic.AddUint64(&nextBondID, 1))
	ch := make(chan bool, len(d.dialers))
	for _, d := range d.dialers {
		go func(d Dialer) {
			conn, err := d.DialContext(ctx, network, addr)
			if err != nil {
				log.Errorf("failed to dial %v: %v", d.Label(), err)
				ch <- false
			}
			var leadBytes [1 + 8]byte
			// version is implicitly set to 0
			binary.LittleEndian.PutUint64(leadBytes[1:], bc.bondID)
			if _, err := conn.Write(leadBytes[:]); err != nil {
				log.Errorf("failed to write lead bytes to %v: %v", d.Label(), err)
				ch <- false
			} else {
				bc.add(conn, true)
				ch <- true
			}
		}(d)
	}
	tries := len(d.dialers)
loop:
	for {
		select {
		case result := <-ch:
			if result {
				return bc, nil
			}
			if tries--; tries == 0 {
				break loop
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("no dailer left")
}

func (d *dialer) Label() string {
	return fmt.Sprintf("bond dialer with %d dialers", len(d.dialers))
}
