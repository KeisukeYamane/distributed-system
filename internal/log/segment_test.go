package log

import (
	"io"
	"os"
	"testing"

	api "github.com/KeisukeYamane/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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

	_, err = s.Append(want)
	// 4回目の実行はc.Segment.MaxIndexBytesを超過するためerr
	require.Equal(t, io.EOF, err)

	// この時点ではインデックスの容量は最大になっている
	require.True(t, s.IsMaxed())
	require.NoError(t, s.Close())

	p, _ := proto.Marshal(want)
	// len(p) = 書き込むバイト長と固定長(なんbyte書き込まれているか)を足して *4することでstoreが現時点で最大になる
	c.Segment.MaxStoreBytes = uint64(len(p)+lenWidth) * 4
	// インデックスの容量を増やす
	c.Segment.InitialOffset = 1024

	s, err = newSegment(dir, initialBaseOffset, c)
	require.NoError(t, err)
	// ストアが最大なのでエラーになる
	require.True(t, s.IsMaxed())

	// removeして各ファイルを削除する
	require.NoError(t, s.Remove())
	// 永続化されたインデックスとストアのファイルからセグメントの状態を読み出すことを確認
	s, err = newSegment(dir, initialBaseOffset, c)
	require.NoError(t, err)
	// 新たにセグメントを作成したので上限にはもちろん達していない
	require.False(t, s.IsMaxed())
	require.NoError(t, s.Close())

}
