package log

import (
	"fmt"
	"io"
	"os"
	"testing"

	api "github.com/KeisukeYamane/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	// key, valueになる key=string, value=func(t *testing.T, log *Log)
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		// 新たにログを作成せずテストすることが可能になる
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "store-test")
			defer os.RemoveAll(dir)
			require.NoError(t, err)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

/**
c.Segment.MaxStoreBytes = 32と設定しているので、以降のテストで書き込んでいるレコードは、
一つのセグメントに二つしか書き込めないことに注意してください。
三つのレコードを書き込むと、二つのセグメントが作成されることになります。
*/

// レコードの追加と読み書きのテスト
func testAppendRead(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	// off, err := log.Append(append)
	// require.NoError(t, err)
	// require.Equal(t, uint64(0), off)

	// -- test --
	for i := 0; i < 7; i++ {
		fmt.Println(i+1, "回目")
		off, err := log.Append(append)
		require.NoError(t, err)
		require.Equal(t, uint64(i), off)

		fmt.Println("len(log.segments)", len(log.segments))
		fmt.Println("log.activeSegment.baseOffset", log.activeSegment.baseOffset)
		fmt.Println("log.activeSegment.nextOffset", log.activeSegment.nextOffset)
		fmt.Println("log.activeSegment.store.size", log.activeSegment.store.size)
		fmt.Println("log.activeSegment.index.size", log.activeSegment.index.size)
		fmt.Println("log.activeSegment.index.mmap", len(log.activeSegment.index.mmap))

		// off = 0
		read, err := log.Read(off)
		require.NoError(t, err)
		require.Equal(t, append.Value, read.Value)
	}

	require.NoError(t, log.Close())

	n, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	fmt.Println("Log Close And exec NewLog")
	fmt.Println("len(log.segments)", len(n.segments))
	fmt.Println("log.activeSegment.baseOffset", n.activeSegment.baseOffset)
	fmt.Println("log.activeSegment.nextOffset", n.activeSegment.nextOffset)
	fmt.Println("log.activeSegment.store.size", n.activeSegment.store.size)
	fmt.Println("log.activeSegment.index.size", n.activeSegment.index.size)
	fmt.Println("log.activeSegment.index.mmap", len(n.activeSegment.index.mmap))
}

// 範囲外エラーのテスト
func testOutOfRangeErr(t *testing.T, log *Log) {
	// off=1はs.nextOffset <= offを満たす
	read, err := log.Read(1)
	require.Nil(t, read)
	require.Error(t, err)
	require.NoError(t, log.Close())
}

// ログを作成した時に、ログのインスタンスが保存したデータからログが再開するかテスト
func testInitExisting(t *testing.T, o *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := o.Append(append)
		require.NoError(t, err)
	}
	require.NoError(t, o.Close())

	fmt.Println("o.segments", o.segments)

	off, err := o.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = o.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	n, err := NewLog(o.Dir, o.Config)
	require.NoError(t, err)

	off, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

// ログのスナップショットを作成したり、ログを復元できたりするように、ディスクに保存されているログを読み込めるかテスト
func testReader(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	// +1される前のnextOffset
	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := io.ReadAll(reader) // func io.ReadAll(r io.Reader) ([]byte, error)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read) // TODO:Unmarshalってどんな引数をうめこめばよかったっけ？
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
	require.NoError(t, log.Close())
}

func testTruncate(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}

	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)
	_, err = log.Read(1)
	require.Error(t, err)
	_, err = log.Read(2)
	require.NoError(t, err)
	require.NoError(t, log.Close())
}
