package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
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
	// 指定したファイル名が構造体の中に格納されているか
	require.Equal(t, f.Name(), idx.Name())

	/*
		無名の構造体を作成する
		単一の場合 struct{define}{value}
		複数の場合 []struct{define}{{value1}, {value2}}
	*/
	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	/*
		上の構造体の内容をそのまま書き込むと
		4byte(0をencord) 8byte(0をencord) 4byte(1をencord) 8byte(10をencord)
	*/
	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}

	// 既存のエントリを超えて読み出す場合、インデックスはエラーを返す
	_, _, err = idx.Read(int64(len(entries)))
	require.Error(t, err)

	// インデックスは、既存のファイルその状態を維持することを確認する
	_ = idx.Close()

	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)

	off, pos, err := idx.Read(-1) // 最後尾読み出し
	require.NoError(t, err)
	require.Equal(t, entries[1].Off, off)
	require.Equal(t, entries[1].Pos, pos)
}
