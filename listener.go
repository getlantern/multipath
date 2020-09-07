package multipath

import (
	"bufio"
	"io"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

type mpListener struct {
	listeners      []net.Listener
	listenerStats  []StatsTracker
	mpConns        map[connectionID]*mpConn
	muMPConns      sync.Mutex
	chNextAccepted chan net.Conn
	startOnce      sync.Once
	chClose        chan struct{}
	closeOnce      sync.Once
}

func MPListener(listeners []net.Listener, stats []StatsTracker) net.Listener {
	mpl := &mpListener{
		listeners:      listeners,
		listenerStats:  stats,
		mpConns:        make(map[connectionID]*mpConn),
		chNextAccepted: make(chan net.Conn),
		chClose:        make(chan struct{}),
	}
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
		go func(l net.Listener, st StatsTracker) {
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
		}(l, mpl.listenerStats[i])
	}
}

func (mpl *mpListener) acceptFrom(l net.Listener, st StatsTracker) error {
	conn, err := l.Accept()
	if err != nil {
		return err
	}
	r := bufio.NewReader(conn)
	var leadBytes [leadBytesLength]byte
	_, err = io.ReadFull(r, leadBytes[:])
	if err != nil {
		return err
	}
	version := uint8(leadBytes[0])
	if uint8(version) != 0 {
		return ErrUnexpectedVersion
	}
	var cid connectionID
	copy(cid[:], leadBytes[1:])
	var newConn bool
	if cid == zeroCID {
		newConn = true
		cid = connectionID(uuid.New())
		copy(leadBytes[1:], cid[:])
		log.Tracef("New connection from %v, assigned CID %v", conn.RemoteAddr(), cid)
	} else {
		log.Tracef("New subflow of CID %v from %v", cid, conn.RemoteAddr())
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

func (mpl *mpListener) remove(cid connectionID) {
	mpl.muMPConns.Lock()
	delete(mpl.mpConns, cid)
	mpl.muMPConns.Unlock()
}
