package lru

type NullCache struct {}

func (NullCache) Add(key uint64, value []byte) bool { return false }

func (NullCache) Get(key uint64) ([]byte, bool) { return nil, false }

func (NullCache) Remove(key uint64) {}
