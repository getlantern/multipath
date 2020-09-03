package multipath

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sort"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/getlantern/ema"
)

type Dialer interface {
	DialContext(ctx context.Context) (net.Conn, error)
	Label() string
}

type Stats interface {
	FormatStats() (stats []string)
}

type subflowDialer struct {
	Dialer
	label            string
	successes        uint64
	consecSuccesses  uint64
	failures         uint64
	framesSent       uint64
	framesRetransmit uint64
	framesRecv       uint64
	bytesSent        uint64
	bytesRetransmit  uint64
	bytesRecv        uint64
	emaRTT           *ema.EMA
}

func (sfd *subflowDialer) DialContext(ctx context.Context) (net.Conn, error) {
	conn, err := sfd.Dialer.DialContext(ctx)
	if err == nil {
		atomic.AddUint64(&sfd.successes, 1)
		atomic.AddUint64(&sfd.consecSuccesses, 1)
	} else {
		// reset RTT to deprioritize this dialer
		sfd.emaRTT.SetDuration(longRTT)
		atomic.AddUint64(&sfd.failures, 1)
		atomic.StoreUint64(&sfd.consecSuccesses, 0)
	}
	return conn, err
}

func (sfd *subflowDialer) onRecv(n uint64) {
	atomic.AddUint64(&sfd.framesRecv, 1)
	atomic.AddUint64(&sfd.bytesRecv, n)
}
func (sfd *subflowDialer) onSent(n uint64) {
	atomic.AddUint64(&sfd.framesSent, 1)
	atomic.AddUint64(&sfd.bytesSent, n)
}
func (sfd *subflowDialer) onRetransmit(n uint64) {
	atomic.AddUint64(&sfd.framesRetransmit, 1)
	atomic.AddUint64(&sfd.bytesRetransmit, n)
}
func (sfd *subflowDialer) updateRTT(rtt time.Duration) {
	sfd.emaRTT.UpdateDuration(rtt)
}

type mpDialer struct {
	name    string
	dialers []*subflowDialer
}

func MPDialer(name string, dialers ...Dialer) Dialer {
	var subflowDialers []*subflowDialer
	for _, d := range dialers {
		subflowDialers = append(subflowDialers, &subflowDialer{Dialer: d, label: d.Label(), emaRTT: ema.NewDuration(longRTT, 0.1)})
	}
	d := &mpDialer{name, subflowDialers}
	return d
}

// DialContext dials the addr using all dialers and returns a connection
// contains subflows from whatever dialers available.
func (mpd *mpDialer) DialContext(ctx context.Context) (net.Conn, error) {
	dialers := mpd.sorted()
	for i, d := range dialers {
		conn, err := d.DialContext(ctx)
		if err != nil {
			log.Errorf("failed to dial %s: %v", d.Label(), err)
			continue
		}
		probeStart := time.Now()
		cid, err := mpd.handshake(conn, 0)
		if err != nil {
			log.Errorf("failed to handshake %s, continuing: %v", d.Label(), err)
			conn.Close()
			continue
		}
		bc := newMPConn(cid)
		bc.add(d.label, conn, true, probeStart, d)
		for _, d := range dialers[i:] {
			go func(d *subflowDialer) {
				conn, err := d.DialContext(ctx)
				if err != nil {
					log.Errorf("failed to dial %s: %v", d.Label(), err)
					return
				}
				probeStart := time.Now()
				_, err = mpd.handshake(conn, cid)
				if err != nil {
					log.Errorf("failed to handshake %s, continuing: %v", d.Label(), err)
					conn.Close()
					return
				}
				bc.add(d.label, conn, true, probeStart, d)
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
	return fmt.Sprintf("multipath dialer to %s with %d paths", mpd.name, len(mpd.dialers))
}

func (mpd *mpDialer) sorted() []*subflowDialer {
	dialersCopy := make([]*subflowDialer, len(mpd.dialers))
	copy(dialersCopy, mpd.dialers)
	sort.Slice(dialersCopy, func(i, j int) bool {
		it := dialersCopy[i].emaRTT.GetDuration()
		jt := dialersCopy[j].emaRTT.GetDuration()
		// basically means both have unknown RTT or fail to dial
		if it == jt {
			return rand.Intn(2) > 0
		}
		return it < jt
	})
	return dialersCopy
}

func (mpd *mpDialer) FormatStats() (stats []string) {
	for _, d := range mpd.sorted() {
		stats = append(stats, fmt.Sprintf("%s  S: %4d(%3d)  F: %4d  RTT: %6.0fms  SENT: %7d/%7s  RECV: %7d/%7s  RT: %7d/%7s",
			d.label,
			atomic.LoadUint64(&d.successes),
			atomic.LoadUint64(&d.consecSuccesses),
			atomic.LoadUint64(&d.failures),
			d.emaRTT.GetDuration().Seconds()*1000,
			atomic.LoadUint64(&d.framesSent), humanize.Bytes(atomic.LoadUint64(&d.bytesSent)),
			atomic.LoadUint64(&d.framesRecv), humanize.Bytes(atomic.LoadUint64(&d.bytesRecv)),
			atomic.LoadUint64(&d.framesRetransmit), humanize.Bytes(atomic.LoadUint64(&d.bytesRetransmit))))
	}
	return
}
