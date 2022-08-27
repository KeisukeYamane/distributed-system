package log

import (
	"fmt"
	"os"
	"path/filepath"

	api "github.com/KeisukeYamane/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

/*
インデックスとストアの操作を統合するために、インデックスとストアをまとめて扱う
ログがアクティブなセグメントにレコードを書き込む場合、セグメントはデータをストアに書き込み、
インデックスに新たなエントリを追加する必要がある(エントリはインデックスの中身、インデックスの中にエントリが複数存在するイメージ)
同様に読み取りの場合、セグメントはインデックスからエントリを検索し、ストアからデータを取り出す必要がある
(ストアとインデックスの協調が必要)

基本、インデックスはストアの付加情報になるのでストアへの操作の次にインデックスへの操作が行われることが多い
*/
type segment struct {
	/*
		セグメントはストアとインデックスを呼び出す必要があるため、
		最初の2つのフィールド(storeとindex)にそれらへのポインタを保持する必要がある
	*/
	store      *store
	index      *index
	baseOffset uint64 // インデックスエントリの相対的なオフセットを計算するためのオフセット
	nextOffset uint64 // 新たなレコードを追加する際のオフセット
	config     Config // ストアファイルとインデックスのサイズを設定された制限値と比較でき、セグメントが最大になったことを知ることが可能
}

/*
ログは現在のアクティブセグメントが最大サイズに達した時など
新たなセグメントを追加する必要がある時に、newSegment関数を呼び出す
*/
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	// storeファイルを取得する baseOffsetを使用
	storeFile, err := os.OpenFile(
		// Join関数(引数同士を/で連結する。もしも引数の先頭や末尾に/が含まれていても取り除かれる)
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")), // storeファイルの拡張子は.store
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)
	if err != nil {
		return nil, err
	}

	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// indexファイルを取得する baseOffSetを使用
	indexFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")), // indexファイルの拡張子は.index
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)
	if err != nil {
		return nil, err
	}

	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		// errが返る場合はindexファイルの中身が何もない時
		s.nextOffset = baseOffset
	} else {
		/*
		 インデックスに少なくとも1つのエントリがある場合、
		 次に書き込まれるレコードのオフセットはセグメントの最後のオフセットを使う必要がある
		 ベースのオフセットと相対オフセットの和に1を加算して取得可能
		 TODO: 全体感が全くわからないので、なぜ加算するのか徹底的に調べること
		*/
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

// セグメントにレコードを書き込む
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur

	// recordはレコードの実態そのもの 一度マーシャリングを行い、byte列に変換する
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// posにはマーシャリングされたレコードを読み出す位置が格納されている(= 何もレコードがない時はもちろんposは0になる)
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	if err = s.index.Write(
		// インデックスのオフセットは、ベースのオフセットからの相対 (全然わからん)
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++ // increment 将来のAppendメソッドの呼び出しに備える

	return cur, nil
}
