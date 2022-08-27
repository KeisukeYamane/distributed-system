package log

import (
	"fmt"
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

	/*
		2回実行した時のメモ
		s.Append(want).off 16
		s.baseOffset       16
		s.nextOffset       17
		s.index.size       12
		s.store.size       23

		s.Append(want).off 17
		s.baseOffset       16
		s.nextOffset       18
		s.index.size       24
		s.store.size       46
	*/

	off, err := s.Append(want)
	fmt.Println("off", off)
	fmt.Println("s.baseOffset", s.baseOffset)
	fmt.Println("s.nextOffset", s.nextOffset)
	fmt.Println("s.index.size", s.index.size)
	fmt.Println("s.store.size", s.store.size)
	// for i := uint64(0); i < 3; i++ {
	// 	off, err := s.Append(want)
	// }

	off, err = s.Append(want)
	fmt.Println("off", off)
	fmt.Println("s.baseOffset", s.baseOffset)
	fmt.Println("s.nextOffset", s.nextOffset)
	fmt.Println("s.index.size", s.index.size)
	fmt.Println("s.store.size", s.store.size)
}
