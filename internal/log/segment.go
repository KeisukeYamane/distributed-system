package log

/*
インデックスとストアの操作を統合するために、インデックスとストアをまとめて扱う
ログがアクティブなセグメントにレコードを書き込む場合、セグメントはデータをストアに書き込み、
インデックスに新たなエントリを追加する必要がある
同様に読み取りの場合、セグメントはインデックスからエントリを検索し、ストアからデータを取り出す必要がある
*/
type Segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}
