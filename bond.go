// Package bond provide a simply way to bond multiple net.Conn to a single one.
//
// Definitions:
//
// - subflow: The byte stream on one of the connections belonging to the bond.
//
// Each subflow is a bidirectional byte stream in the following form until the
// connection ends.
//
//       ----------------------------------------------
//      |  version(1)  |  bond id(8)  |  frames (...)  |
//       ----------------------------------------------
//
// There are two types of frames. Data frame carries application data while ack
// frame carries acknowledgement to the frame just received. Payload size and
// frame number uses variable-length integer encoding as described here:
// https://tools.ietf.org/html/draft-ietf-quic-transport-29#section-16
//
//       --------------------------------------------------------
//      |  payload size(1-8)  |  frame number (1-8)  |  payload  |
//       --------------------------------------------------------
//
//       ---------------------------------------
//      |  00000000  |  ack frame number (1-8)  |
//       ---------------------------------------
//
// Ack frames with frame number < 10 are reserved for control. For now only 0
// and 1 are used, for ping and pong frame respectively.  They are used to
// update RTT on inactive subflows and detect recovered subflows.
//
// Ping frame:
//       -------------------------
//      |  00000000  |  00000000  |
//       -------------------------
//
// Pong frame:
//       -------------------------
//      |  00000000  |  00000001  |
//       -------------------------

package bond

import (
	"bytes"
	"errors"

	"github.com/getlantern/golog"
	pool "github.com/libp2p/go-buffer-pool"
)

const (
	minFrameNumber uint64 = 10
	frameTypePing  uint64 = 0
	frameTypePong  uint64 = 1
)

var (
	ErrUnexpectedVersion = errors.New("unexpected version")
	ErrUnexpectedBondID  = errors.New("unexpected bond ID")
	log                  = golog.LoggerFor("bond")
)

type frame struct {
	fn    uint64
	bytes []byte
}

type sendFrame struct {
	fn              uint64
	buf             []byte
	isDataFrame     bool
	retransmissions int
}

func composeFrame(fn uint64, b []byte) sendFrame {
	sz := len(b)
	buf := pool.Get(8 + 8 + sz)
	wb := bytes.NewBuffer(buf[:0])
	WriteVarInt(wb, uint64(sz))
	WriteVarInt(wb, fn)
	if sz > 0 {
		wb.Write(b)
	}
	return sendFrame{fn: fn, buf: wb.Bytes(), isDataFrame: sz > 0}
}
