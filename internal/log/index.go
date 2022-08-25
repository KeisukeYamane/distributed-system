package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

/*
サービスを起動すると、サービスはログに追加される次のレコードに設定するオフセットを知る必要があります。
サービスは、インデックスの最後のエントリを見て次のレコードのオフセットを知ることができ、それはファイルの最後の12バイトを読み出すだけの簡単な処理です。
しかし、メモリへマップするためにファイルを大きくすると、その処理が狂ってしまいます(最初にサイズを 変更する理由は、一度メモリにマップされたファイルはサイズを変更できないからです)。
ファイルの最後に空領域を追加してファイルを大きくするので、最後のエントリはファイルの終わりではなく、最後のエントリとファイルの終わりの間には使われていない領域が存在することになります。
その領域が残ってしまうと、サービスを正しく再起動できません。そのため、サービスを停止する際には、インデックスファイルを切り詰めて空領域を取り除き、最後のエントリを再びファイルの最後になるようにしています。
*/

/*
インデックスエントリを構成するバイト数を定義
インデックスエントリにはレコードのオフセットとストアファイル内の位置という2つの
フィールドがある
オフセットはuint32, 位置はuint64として保存するので、それぞれ4バイトと8バイトの領域を使用する
*/
const (
	offWidth = 4                   // レコードのオフセット
	posWidth = 8                   // ストアファイル内の位置
	entWidth = offWidth + posWidth // エントリー長
)

// インデックスファイルを定義 永続化されたファイルとメモリマップされたファイルから構成される
type index struct {
	file *os.File
	// メモリマップトファイル
	mmap gommap.MMap
	// sizeはインデックスのサイズであり、同時に次にインデックスに追加されるエントリをどこに書き込むかを表している(ストアでも同じような処理を書いた)
	size uint64
}

// Config = 指定されたファイルからindexを作成する
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// ファイルの現在のサイズを保存することで、インデックスエントリを追加する際に、インデックスファイル内のデータ量を管理することができる
	idx.size = uint64(fi.Size())
	// Truncate → 指定したファイルのファイルサイズを指定したサイズにする
	if err := os.Truncate(
		// ファイルをメモリへマップする前に、ファイルを最大のインデックスサイズまで大きくする(一度メモリに配置してしまうとファイルサイズを変更できないため)
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	/*
		index構造体が保持しているfileフィールドから、ファイルディスクリプタを使用し
		ファイル全体をメモリへマッピングする
	*/
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, err
}

// インデックスのファイルパスを返す
func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) Close() error {
	/*
	 メモリ領域に配置されたファイルの変更をデバイスにフラッシュする。
	 このメソッドを呼び出さない場合、領域がアンマップされる前に変更がフラッシュされる保証はない。
	 flags パラメータは、フラッシュを同期的に行うか（メソッドが戻る前に）MS_SYNC、
	 または非同期的に行うか（フラッシュは単にスケジュールされる）MS_ASYNC で指定する。
	*/
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	/*
		Sync は、ファイルの現在の内容を安定したストレージにコミットします。
		通常、これは最近書き込まれたデータのファイルシステムのインメモリコピーを
		ディスクにフラッシュすることを意味する。
	*/
	if err := i.file.Sync(); err != nil {
		return err
	}
	// 一度最大までファイルの容量を増やしていたので、実際に書き込まれた容量に合わせて切り詰める
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	// i.mmap.Syncでファイルに同期をかける、そしてi.file.Sync()でファイルをストレージに保存し、その後ファイルの容量を調整する

	return i.file.Close()
}

// オフセットを受け取り、ストア内の関連したレコードの位置を返す
// 0は常にインデックスの最初のエントリのオフセット、1は2番目のエントリと続く
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	// -1指定されたら最後のエントリ番号を返す(-1自体にあまり意味はなさそう)
	if in == -1 {
		// outはエントリ
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	// posは位置を表す、エントリ * 固定長で読み出す位置を把握することができる
	pos = uint64(out) * entWidth
	// posが最後らへんの場合、entWidth(固定長)が必ず存在しないといけないが、ファイルのサイズを超過する場合はエラーとする
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	// エントリとその位置がわかったらメモリにマッピングされたファイルがどの位置にあるか返す
	out = enc.Uint32(i.mmap[pos : pos+offWidth])          // レコードのオフセット pos=12, pos+offWidth=16 引くと4
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth]) // ストアファイル内の位置 pos+offWidth=16, pos+entWidth=24 引くと8

	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if i.isMaxed() {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	// 次の書き込みが行われる順番を保持する
	i.size += uint64(entWidth)

	return nil
}

// 書き込む領域があるかどうか確認する
// TODO: 動かして内部の動きを確認すること
func (i *index) isMaxed() bool {
	return uint64(len(i.mmap)) < i.size+entWidth
}
