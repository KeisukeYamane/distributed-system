package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

/*
ログに関わるテストは繰り返し読むことができるという点が重要になるため、for文を使い確認を行う
*/

var (
	// 書き込む文字列
	write = []byte("hello world")
	// レコード長　書き込むバイト数 + 固定長
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	defer os.Remove(f.Name())

	require.NoError(t, err)

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s) // ストアへの追加テスト
	testRead(t, s)   // ストアで読み出し
	testReadAt(t, s) // 位置を指定して読み出し

	s, err = newStore(f)
	require.NoError(t, err)
	// 再びストアを作成、ストアからの読み出しをテストすることで、サービスが再起動後に状態を回復することを検証
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	// TODO: よくわからんので調べること
	t.Helper()

	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		// pos + n(書き込みバイト数) = width * i(繰り返し書き込むためloop変数でかける)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()

	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)

		pos += width // widthは書き込みバイト数 + 固定長
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()

	// 前半は固定長を読み、その後て固定長のバイトから可変長のバイト数を取得すし、可変長の内容を取得する
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth) // 固定長分のバイトを用意
		n, err := s.ReadAt(b, off)  // 固定長分のバイトを読む
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		off += int64(n)

		size := enc.Uint64(b) // 可変長のバイト数を取得
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)

		require.Equal(t, write, b) // write=hello worldをbyteに直したもの
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	defer os.Remove(f.Name())

	require.NoError(t, err)

	s, err := newStore(f)
	require.NoError(t, err)
	_, _, err = s.Append(write)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close() // Closeメソッドを挟むことでバッファをフラッシュする(= Appendされた内容はバッファされディスクに書き込まれていない)
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)

	if err != nil {
		return nil, 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return f, fi.Size(), nil
}
