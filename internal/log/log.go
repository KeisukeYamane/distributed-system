package log

import (
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

//　セグメントの集まりを管理する
type Log struct {
	mu sync.RWMutex // 左記を使用すると「読み取り時」は他の処理がまたされることなく参照が可能

	Dir    string // セグメントを保持する
	Config Config

	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setUp()
}

func (l *Log) setUp() error {
	files, err := os.ReadDir(l.Dir) // 該当のディレクトリ内のファイル名を全て返す もし途中でエラーが発生した場合、途中まで読み込んでいたファイル名を返す
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix( // 第一引数の末尾(suffix)から第二引数で与えられた文字列を取り除く 該当の文字列がない場合はそのまま返す
			file.Name(),
			path.Ext(file.Name()), // pathで使用されてるファイル名の拡張子を返す /testA/testB/main.go -> .go
		)

		// 文字列を十進数のint型に変換 ex: strconv.ParseUint(offStr, 10, 64) -> 十進数のint64型に変換
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)

		// offsetが大きい順に並べ替える
		sort.Slice(baseOffsets, func(i, j int) bool {
			return baseOffsets[i] < baseOffsets[j]
		})
		for i := 0; i < len(baseOffsets); i++ {
			if err = l.newSegment(baseOffsets[i]); err != nil {
				return err
			}

			// baseOffsetsは、インデックスとストアの2つの重複を含んでいるので、重複しているものはスキップする
			// TODO: ?? 意味がわからない
			i++
		}
	}
}
