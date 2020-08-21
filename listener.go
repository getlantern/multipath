package bond

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type bondListener struct {
	nextBondID     uint64
	listeners      []net.Listener
	bondConns      map[uint64]*bondConn
	muBondConns    sync.Mutex
	chNextAccepted chan net.Conn
	startOnce      sync.Once
	chClose        chan struct{}
	closeOnce      sync.Once
}

func BondListener(ls ...net.Listener) *bondListener {
	return &bondListener{listeners: ls, bondConns: make(map[uint64]*bondConn), chNextAccepted: make(chan net.Conn), chClose: make(chan struct{})}
}

func (bl *bondListener) Accept() (net.Conn, error) {
	bl.startOnce.Do(bl.start)
	return <-bl.chNextAccepted, nil
}

func (bl *bondListener) Close() error {
	bl.closeOnce.Do(func() { close(bl.chClose) })
	return nil
}

func (bl *bondListener) Addr() net.Addr {
	panic("not implemented")
}

func (bl *bondListener) start() {
	for _, l := range bl.listeners {
		go func(l net.Listener) {
			for {
				if err := bl.acceptFrom(l); err != nil {
					select {
					case <-bl.chClose:
						return
					default:
						log.Debugf("failed to accept from %v: %v", l.Addr(), err)
					}
				}
			}
		}(l)
	}
}

func (bl *bondListener) acceptFrom(l net.Listener) error {
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
	bondID := binary.LittleEndian.Uint64(leadBytes[1:])
	var newBond bool
	if bondID == 0 {
		newBond = true
		bondID = atomic.AddUint64(&bl.nextBondID, 1)
		binary.LittleEndian.PutUint64(leadBytes[1:], bondID)
		log.Tracef("New bond from %v, assigned bond ID %v", conn.RemoteAddr(), bondID)
	} else {
		log.Tracef("New connection of bond ID %v from %v", bondID, conn.RemoteAddr())
	}
	probeStart := time.Now()
	// echo lead bytes back to the client
	if _, err := conn.Write(leadBytes[:]); err != nil {
		return err
	}
	var bc *bondConn
	bl.muBondConns.Lock()
	if newBond {
		bc = newBondConn(bondID)
		bl.bondConns[bondID] = bc
	} else {
		bc = bl.bondConns[bondID]
	}
	bl.muBondConns.Unlock()
	bc.add(conn, false, probeStart)
	if newBond {
		bl.chNextAccepted <- bc
	}
	return nil
}

func (bl *bondListener) remove(bondID uint64) {
	bl.muBondConns.Lock()
	delete(bl.bondConns, bondID)
	bl.muBondConns.Unlock()
}
