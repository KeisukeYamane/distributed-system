package log

import (
	"os"
	"testing"

	api "github.com/KeisukeYamane/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

const initialBaseOffset = 16

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024         // storeファイルのバイト数 1024
	c.Segment.MaxIndexBytes = entWidth * 3 // indexのバイト数 12 * 3 = 36

	// baseOffsetは16
	s, err := newSegment(dir, initialBaseOffset, c)
	require.NoError(t, err)
	require.Equal(t, uint64(initialBaseOffset), s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, initialBaseOffset+i, off)

		// 同じ内容を何度もfor文で書き込む
		got, err := s.Read(off)
		require.NoError(t, err)
		// 取得できる内容はいつも同じ
		require.Equal(t, want.Value, got.Value)
	}
}
