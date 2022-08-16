// ※ 処理を確認する用のファイル

// レコード(処理ログの実体)を保存するファイル
package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

var (
	// レコードサイズとインデックスエントリを永続化するためのエンコーディングを定義
	enc = binary.BigEndian
)

const (
	// レコードの長さを格納するために使うバイト数
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// 与えられたファイルに対するstoreを作成する
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size, // 現在のファイルのサイズを保持することで、データを含むファイルからstoreを再生成することができる(ex: 再起動時など)
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	// len(p)=5の場合[0 0 0 0 0 0 0 5]
	// 上記の記述があることで、何バイト分読み出せば良いのかを把握することができる
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// wにはバイト数が入る p=5bytes w=5
	// 上で長さをバイナリ変換しているので、helloの場合 s.buf=[0 0 0 0 0 0 0 5 104 101 108 108 111]
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// ex: 5 + lenWidth(= 8) = 13
	w += lenWidth
	// 現在のファイルサイズに追加分のバイト数 + 固定のバイト数を足した値をいれる
	s.size += uint64(w)
	// 追加バイト数とそのバイトを読み出す際のポジションを返す
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// 固定長 + 可変長の組み合わせなので、まずは固定長のbyteを確保する
	size := make([]byte, lenWidth)
	// 指定の位置から固定長分読み込み、レコードのbyteを確保する
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// enc.Uint64()でレコードのbyteを取得し、そのbyte分を用意する
	b := make([]byte, enc.Uint64(size))
	// 指定の位置から8byteを足した位置からsize byte分読み取る
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

func main() {
	f, err := os.Create("./text.txt")
	if err != nil {
		fmt.Println(err)
	}

	s, err := newStore(f)
	if err != nil {
		fmt.Println(err)
	}

	p := []byte("Hello") // 5bytes
	n, pos, err := s.Append(p)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("----- Append ----")
	fmt.Println("追加したバイト数", n)
	fmt.Println("読み出しのポジション〜byteから", pos)
	fmt.Println("ファイルのサイズ", s.size)

	fmt.Println("----- Read ----")
	b, err := s.Read(pos)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(b)
}
