package multipath

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
	DialContext(ctx context.Context) (net.Conn, error)
	Label() string
}

type mpDialer struct {
	dialers []Dialer
}

func MPDialer(dialers ...Dialer) Dialer {
	return &mpDialer{dialers}
}

// DialContext dials the addr using all dialers and returns a connection
// contains subflows from whatever dialers available.
func (mpd *mpDialer) DialContext(ctx context.Context) (net.Conn, error) {
	for i, d := range mpd.dialers {
		conn, err := d.DialContext(ctx)
		if err != nil {
			log.Errorf("failed to dial %v: %v", d.Label(), err)
			continue
		}
		probeStart := time.Now()
		cid, err := mpd.handshake(conn, 0)
		if err != nil {
			log.Errorf("failed to handshake %v, continuing: %v", d.Label(), err)
			conn.Close()
			continue
		}
		bc := newMPConn(cid)
		bc.add(conn, true, probeStart)
		for _, d := range mpd.dialers[i:] {
			go func(d Dialer) {
				conn, err := d.DialContext(ctx)
				if err != nil {
					log.Errorf("failed to dial %v: %v", d.Label(), err)
					return
				}
				probeStart := time.Now()
				_, err = mpd.handshake(conn, cid)
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

// handshake exchanges version and cid with the peer and returns the connnection ID
// both end agrees if no error happens.
func (mpd *mpDialer) handshake(conn net.Conn, cid uint64) (uint64, error) {
	var leadBytes [1 + 8]byte
	// the first byte, version, is implicitly set to 0
	if cid != 0 {
		binary.LittleEndian.PutUint64(leadBytes[1:], cid)
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
	newCID := binary.LittleEndian.Uint64(leadBytes[1:])
	if cid != 0 && cid != newCID {
		return 0, ErrUnexpectedCID
	}
	return newCID, nil
}

func (mpd *mpDialer) Label() string {
	return fmt.Sprintf("multipath dialer with %d dialers", len(mpd.dialers))
}
