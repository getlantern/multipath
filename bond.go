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
// There are two types of frames - data frame and ack frame. Data frame carries
// application data while ack frame carries acknowledgement to the frame just
// received. An ack frame with frame number 0 is for probing, so data frame
// number always starts with 1.
//
//       --------------------------------------------------------
//      |  payload size(1-8)  |  frame number (1-8)  |  payload  |
//       --------------------------------------------------------
//
//       ---------------------------------------
//      |  00000000  |  ack frame number (1-8)  |
//       ---------------------------------------

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
