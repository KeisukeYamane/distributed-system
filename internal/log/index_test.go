package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInde(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	defer os.Remove(f.Name())

	require.NoError(t, err)

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(f, c)
	require.NoError(t, err)

	_, _, err = idx.Read(-1)
	// fileサイズが0なのでEOF(= error)が返ることを確認
	require.Error(t, err)
}
