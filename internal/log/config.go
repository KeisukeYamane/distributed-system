package log

// ログの設定を一元的に管理する

// structの入れ子にすることでConfig.Segment.MaxindexBytesのようにアクセスが可能
type Config struct {
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
		InitialOffset uint64
	}
}
