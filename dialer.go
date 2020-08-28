package multipath

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"time"

	"github.com/getlantern/ema"
)

type Dialer interface {
	DialContext(ctx context.Context) (net.Conn, error)
	Label() string
}

type subflowDialer struct {
	Dialer
	emaRTT *ema.EMA
}

func (sfd *subflowDialer) updateRTT(rtt time.Duration) {
	sfd.emaRTT.UpdateDuration(rtt)
}

type mpDialer struct {
	dialers []*subflowDialer
}

func MPDialer(dialers ...Dialer) Dialer {
	var subflowDialers []*subflowDialer
	for _, d := range dialers {
		subflowDialers = append(subflowDialers, &subflowDialer{d, ema.NewDuration(time.Second, 0.1)})
	}
	return &mpDialer{subflowDialers}
}

// DialContext dials the addr using all dialers and returns a connection
// contains subflows from whatever dialers available.
func (mpd *mpDialer) DialContext(ctx context.Context) (net.Conn, error) {
	dialersCopy := make([]*subflowDialer, len(mpd.dialers))
	copy(dialersCopy, mpd.dialers)
	sort.Slice(dialersCopy, func(i, j int) bool {
		return dialersCopy[i].emaRTT.GetDuration() > dialersCopy[j].emaRTT.GetDuration()
	})

	for i, d := range dialersCopy {
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
		bc.add(conn, true, probeStart, d.updateRTT)
		for _, d := range mpd.dialers[i:] {
			go func(d *subflowDialer) {
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
				bc.add(conn, true, probeStart, d.updateRTT)
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
