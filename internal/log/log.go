package log

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/KeisukeYamane/proglog/api/v1"
)

/*
ログの開始処理として、ディスク上のセグメントの一覧を取得する
ファイル名からベースオフセットの値を求めてセグメントのスライスを古い順にソートをかける
既存のセグメントがない場合はnewSegemntを使用し、渡されたベースオフセットを使用して最初のセグメントを作成する
*/

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
	}

	// offsetが大きい順に並べ替える
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	// len(baseOffsets) = dir内のファイル数になる
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}

		// baseOffsetsは、インデックスとストアの2つの重複を含んでいるので、重複しているものはスキップする
		// TODO: ?? 意味がわからない
		i++
	}

	// segmentが全くない場合
	if l.segments == nil {
		if err = l.newSegment(
			l.Config.Segment.InitialOffset,
		); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	// 書き込み・読み込みを許可しない
	l.mu.Lock()
	defer l.mu.Unlock()

	// 最も高い(最後の)オフセットを取得
	highestOffset, err := l.highestOffset()
	if err != nil {
		return 0, err
	}

	fmt.Println("highestOffset", highestOffset)
	fmt.Println("l.activeSegment", l.activeSegment)
	fmt.Println("l.activeSegment.IsMaxed", l.activeSegment.IsMaxed())
	// アクティブセグメントの容量がいっぱいでログが追加できない場合
	if l.activeSegment.IsMaxed() {
		// newSegmentを実行すると作成されたセグメントが新たなアクティブセグメントになる
		err = l.newSegment(highestOffset + 1) // 最後+1で新たにセグメントを作成 引数がsegmentのbaseOffsetになる
		if err != nil {
			return 0, err
		}
	}

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	return off, err
}

/*
segmentのnewSegmentを実行するヘルパメソッド
新たに作成されたセグメントをsegmentsスライスに追加し
その作成したセグメントをアクティブセグメントとする
*/
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, s)
	l.activeSegment = s

	return nil
}

/*
LowestOffset()とHighestOffset()を使用することで、ログに保存されているオフセット範囲を取得できる
レプリケーションを行う連携型クラスタのサポートに取り組む際に、どのノードが最も古いデータと
最新のデータを持っているのか、ログに保存されているオフセットの範囲が重要な情報になる
*/
// l.segments[0]を取るってことは、古い順にセグメントが並んでいることが前提になったコード？？
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.highestOffset()
}

/*
最大オフセットがlowestよりも小さいセグメントをすべて削除する
ディスク容量は無限ではないため、定期的にTruncate()を呼び出し、すでに処理済みの
データの内、不要になった古いセグメントを削除する
*/
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}

	l.segments = segments

	return nil
}

/*
ログ全体を読み込むためのio.Readerを返す
Readerメソッドはio.MultiReaderを使ってセグメントのストアを連結している
セグメントのストアはoriginReader型で定義
① io.Readerインターフェースを満たすことで、io.MultiReader呼び出しに渡すため
② ストアの最初から読み込みを開始し、そのファイル全体を読み込むことを保証するため
*/
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// segment分io.Readerスライスを作成
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReder{segment.store, 0}
	}

	// 入力されたReaderを論理的に連結したReaderを返し、順次読み込む
	return io.MultiReader(readers...)
}

type originReder struct {
	*store
	off int64
}

func (o *originReder) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)

	return n, err
}

// セグメントの中の最大オフセットを取得する
func (l *Log) highestOffset() (uint64, error) {
	// TODO: -1をなぜするのか徹底的に調べること
	off := l.segments[len(l.segments)-1].nextOffset

	if off == 0 {
		return 0, nil
	}

	// TODO: -1をなぜするのか徹底的に調べること
	return off - 1, nil
}

// 指定されたオフセットに保存されているレコードを読み出す
func (l *Log) Read(off uint64) (*api.Record, error) {
	// 読み込みの場合のみロックをかけない、該当の資源の書き込みはロックされる
	l.mu.RLock()
	defer l.mu.RUnlock()

	/*
		指定されたレコードを含むセグメントを見つける
		セグメントは古い順に並んでおり、セグメントのbaseOffsetはセグメント内の最小オフセットなので
		baseOffsetより以上、かつnextOffsetより小さい最初のセグメントを探す
		セグメントを見つけたら、そのセグメントのインデックスからインデックスエントリを取得し、
		ストアファイルからデータを読み出して、そのデータを呼び出し元に返す
	*/
	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}

	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range; %d", off)
	}

	return s.Read(off)
}

// セグメント全てをクローズする
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segement := range l.segments {
		if err := segement.Close(); err != nil {
			return err
		}
	}

	return nil
}

// ログをクローズして、そのデータを削除する
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.Dir)
}

// ログを削除して置き換える新たなログを作成する
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}

	return l.setUp()
}
