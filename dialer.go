package bond

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

type Dialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
	Label() string
}

type bondDialer struct {
	dialers []Dialer
}

func BondDialer(dialers ...Dialer) Dialer {
	return &bondDialer{dialers}
}

// DialContext dials the addr using all dialers and returns a bond contains
// connections from whatever dialers available.
func (bd *bondDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	for i, d := range bd.dialers {
		conn, err := d.DialContext(ctx, network, addr)
		if err != nil {
			log.Errorf("failed to dial %v: %v", d.Label(), err)
			continue
		}
		probeStart := time.Now()
		bondID, err := bd.handshake(conn, 0)
		if err != nil {
			log.Errorf("failed to handshake %v, continuing: %v", d.Label(), err)
			conn.Close()
			continue
		}
		bc := newBondConn(bondID)
		bc.add(conn, true, probeStart)
		for _, d := range bd.dialers[i:] {
			go func(d Dialer) {
				conn, err := d.DialContext(ctx, network, addr)
				if err != nil {
					log.Errorf("failed to dial %v: %v", d.Label(), err)
					return
				}
				probeStart := time.Now()
				_, err = bd.handshake(conn, bondID)
				if err != nil {
					log.Errorf("failed to handshake %v, continuing: %v", d.Label(), err)
					conn.Close()
					return
				}
				bc.add(conn, true, probeStart)
			}(d)
		}
		return bc, nil
	}
	return nil, errors.New("no dailer left")
}

// handshake exchanges version and bondID with the peer and returns the bond ID
// both end agrees if no error happens.
func (bd *bondDialer) handshake(conn net.Conn, bondID uint64) (uint64, error) {
	var leadBytes [1 + 8]byte
	// the first byte, version, is implicitly set to 0
	if bondID != 0 {
		binary.LittleEndian.PutUint64(leadBytes[1:], bondID)
	}
	_, err := conn.Write(leadBytes[:])
	if err != nil {
		return 0, err
	}
	_, err = io.ReadFull(conn, leadBytes[:])
	if err != nil {
		return 0, err
	}
	version := uint8(leadBytes[0])
	if uint8(version) != 0 {
		return 0, ErrUnexpectedVersion
	}
	newBondID := binary.LittleEndian.Uint64(leadBytes[1:])
	if bondID != 0 && bondID != newBondID {
		return 0, ErrUnexpectedBondID
	}
	return newBondID, nil
}

func (bd *bondDialer) Label() string {
	return fmt.Sprintf("bond dialer with %d dialers", len(bd.dialers))
}
