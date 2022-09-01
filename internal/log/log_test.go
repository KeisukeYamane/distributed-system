package log

import (
	"os"
	"testing"

	api "github.com/KeisukeYamane/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	// key, valueになる key=string, value=func(t *testing.T, log *Log)
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		// "reader":                            testReader,
		// "truncate":                          testTruncate,
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
-> どゆこと？？
*/

// レコードの追加と読み書きのテスト
func testAppendRead(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	// off = 0
	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
	require.NoError(t, log.Close())
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

// func testReader(t *testing.T, log *Log) {

// }

// func testTruncate(t *testing.T, log *Log) {

// }
