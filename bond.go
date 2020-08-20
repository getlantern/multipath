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
// frame number starts from 1 and uses variable-length integer encoding as
// described here:
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
// Probe frame is a special type of ack frame which has a frame number of 0.
// It's for updating RTT on inactive subflows and detecting recovered subflows.
//
//       -------------------------
//      |  00000000  |  00000000  |
//       -------------------------

package bond

import (
	"errors"
	"time"

	"github.com/getlantern/golog"
)

var (
	ErrUnexpectedVersion = errors.New("unexpected version")
	ErrUnexpectedBondID  = errors.New("unexpected bond ID")
	log                  = golog.LoggerFor("bond")
)

type frame struct {
	fn          uint64
	bytes       []byte
	firstSentAt time.Time // used only by the sender
}
