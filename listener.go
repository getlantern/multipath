package multipath

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type mpListener struct {
	nextCID        uint64
	listeners      []net.Listener
	mpConns        map[uint64]*mpConn
	muMPConns      sync.Mutex
	chNextAccepted chan net.Conn
	startOnce      sync.Once
	chClose        chan struct{}
	closeOnce      sync.Once
}

func MPListener(ls ...net.Listener) net.Listener {
	return &mpListener{listeners: ls, mpConns: make(map[uint64]*mpConn), chNextAccepted: make(chan net.Conn), chClose: make(chan struct{})}
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
	for _, l := range mpl.listeners {
		go func(l net.Listener) {
			for {
				if err := mpl.acceptFrom(l); err != nil {
					select {
					case <-mpl.chClose:
						return
					default:
						log.Debugf("failed to accept from %s: %v", l.Addr(), err)
					}
				}
			}
		}(l)
	}
}

func (mpl *mpListener) acceptFrom(l net.Listener) error {
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
	bc.add(conn.RemoteAddr().String(), conn, false, probeStart, nullStatsTracker{})
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
