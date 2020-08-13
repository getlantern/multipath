package bond

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAck(t *testing.T) {
	bc := newBondConn(0)
	bc.pendingAck[0] = time.Now()
	bc.acked(0)
	assert.True(t, bc.isAcked(0))
}
