package multipath

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
)

type mpListener struct {
	nextCID        uint64
	listeners      []net.Listener
	listenerStats  []stats
	mpConns        map[uint64]*mpConn
	muMPConns      sync.Mutex
	chNextAccepted chan net.Conn
	startOnce      sync.Once
	chClose        chan struct{}
	closeOnce      sync.Once
}

func MPListener(ls ...net.Listener) net.Listener {
	mpl := &mpListener{listeners: ls,
		listenerStats:  make([]stats, len(ls)),
		mpConns:        make(map[uint64]*mpConn),
		chNextAccepted: make(chan net.Conn),
		chClose:        make(chan struct{}),
	}
	for i, l := range ls {
		mpl.listenerStats[i].label = fmt.Sprintf("%10s(%s)",
			l.Addr().String(), l.Addr().Network())
	}
	tk := time.NewTicker(time.Minute)
	go func() {
		for {
			select {
			case <-mpl.chClose:
				return
			case <-tk.C:
				for _, line := range mpl.FormatStats() {
					log.Debug(line)
				}
			}
		}
	}()
	return mpl
}

func (mpl *mpListener) Accept() (net.Conn, error) {
	mpl.startOnce.Do(mpl.start)
	return <-mpl.chNextAccepted, nil
}

func (mpl *mpListener) Close() error {
	mpl.closeOnce.Do(func() { close(mpl.chClose) })
	return nil
}

// Addr satisfies the net.Listener interface. It returns the zero value of
// net.IPAddr.
func (mpl *mpListener) Addr() net.Addr {
	return &net.IPAddr{}
}

func (mpl *mpListener) start() {
	for i, l := range mpl.listeners {
		go func(l net.Listener, st statsTracker) {
			for {
				if err := mpl.acceptFrom(l, st); err != nil {
					select {
					case <-mpl.chClose:
						return
					default:
						log.Debugf("failed to accept from %s: %v", l.Addr(), err)
					}
				}
			}
		}(l, &mpl.listenerStats[i])
	}
}

func (mpl *mpListener) acceptFrom(l net.Listener, st statsTracker) error {
	conn, err := l.Accept()
	if err != nil {
		return err
	}
	r := bufio.NewReader(conn)
	var leadBytes [1 + 8]byte
	_, err = io.ReadFull(r, leadBytes[:])
	if err != nil {
		return err
	}
	version := uint8(leadBytes[0])
	if uint8(version) != 0 {
		return ErrUnexpectedVersion
	}
	cid := binary.LittleEndian.Uint64(leadBytes[1:])
	var newConn bool
	if cid == 0 {
		newConn = true
		cid = atomic.AddUint64(&mpl.nextCID, 1)
		binary.LittleEndian.PutUint64(leadBytes[1:], cid)
		log.Tracef("New connection from %v, assigned CID %d", conn.RemoteAddr(), cid)
	} else {
		log.Tracef("New subflow of CID %d from %v", cid, conn.RemoteAddr())
	}
	probeStart := time.Now()
	// echo lead bytes back to the client
	if _, err := conn.Write(leadBytes[:]); err != nil {
		return err
	}
	var bc *mpConn
	mpl.muMPConns.Lock()
	if newConn {
		bc = newMPConn(cid)
		mpl.mpConns[cid] = bc
	} else {
		bc = mpl.mpConns[cid]
	}
	mpl.muMPConns.Unlock()
	bc.add(conn.RemoteAddr().String(), conn, false, probeStart, st)
	if newConn {
		mpl.chNextAccepted <- bc
	}
	return nil
}

func (mpl *mpListener) remove(cid uint64) {
	mpl.muMPConns.Lock()
	delete(mpl.mpConns, cid)
	mpl.muMPConns.Unlock()
}

func (mpd *mpListener) FormatStats() (stats []string) {
	for _, st := range mpd.listenerStats {
		stats = append(stats, fmt.Sprintf("%s  SENT: %7d/%7s  RECV: %7d/%7s  RT: %7d/%7s",
			st.label,
			atomic.LoadUint64(&st.framesSent), humanize.Bytes(atomic.LoadUint64(&st.bytesSent)),
			atomic.LoadUint64(&st.framesRecv), humanize.Bytes(atomic.LoadUint64(&st.bytesRecv)),
			atomic.LoadUint64(&st.framesRetransmit), humanize.Bytes(atomic.LoadUint64(&st.bytesRetransmit))))
	}
	return
}

type stats struct {
	label            string
	framesSent       uint64
	framesRetransmit uint64
	framesRecv       uint64
	bytesSent        uint64
	bytesRetransmit  uint64
	bytesRecv        uint64
}

func (s *stats) onRecv(n uint64) {
	atomic.AddUint64(&s.framesRecv, 1)
	atomic.AddUint64(&s.bytesRecv, n)
}
func (s *stats) onSent(n uint64) {
	atomic.AddUint64(&s.framesSent, 1)
	atomic.AddUint64(&s.bytesSent, n)
}
func (s *stats) onRetransmit(n uint64) {
	atomic.AddUint64(&s.framesRetransmit, 1)
	atomic.AddUint64(&s.bytesRetransmit, n)
}
func (s *stats) updateRTT(time.Duration) {
}
