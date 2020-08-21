// Package bond provides a simply way to bond multiple connections (subflows)
// together to a single one for throughput and reliability.
////
// Each subflow is a bidirectional byte stream each side in the following form
// until the connection ends. When establishing the very first subflow, the
// client sends an all-zero bond ID and server assigns a bond ID and sends it
// it back. Subsequent subflows all use the same bond ID.
//
//       ----------------------------------------------
//      |  version(1)  |  bond id(8)  |  frames (...)  |
//       ----------------------------------------------
//
// There are two types of frames. Data frame carries application data while ack
// frame carries acknowledgement to the frame just received. When one data
// frame is not acked in time, bond tries to send the data frame via another
// subflow, until all subflows have been tried. Payload size and frame number
// uses variable-length integer encoding as described here:
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
// and 1 are used, for ping and pong frame respectively. They are for updating
// RTT on inactive subflows and detecting recovered subflows.
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
