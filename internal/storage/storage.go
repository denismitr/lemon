package storage

import (
	"github.com/pkg/errors"
	"os"
)

const (
	DefaultFilePerm os.FileMode = 0666
)

type Storage interface {
	PKs() []string
	GetValueAt(offset int) ([]byte, error)
	RemoveAt(offset int) error
	ReplaceValueAt(offset int, v []byte) error
	Append(k string, v []byte)
	Initialize() error
	Persist() error
	Load() error
	Len() int
	LastOffset() int
}

type PrimaryKeys []string

type Value string

func (i Value) String() string {
	return string(i)
}

type Model struct {
	PKs    PrimaryKeys `json:"pks"`
	Values []Value     `json:"documents"`
}

type Item struct {
	Key string
	Value Value
}

func FileExists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}

	return true
}

type FileCloser func() error

func FileNopCloser() error { return nil }

func CreateFileUnderLock(path string, perm os.FileMode) (*os.File, FileCloser, error) {
	mode := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	f, err := os.OpenFile(path, mode, perm)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "could not open file %s", path)
	}

	closer := func() error {
		return f.Close()
	}

	return f, closer, nil
}

func OpenFile(path string) (*os.File, FileCloser, error) {
	if f, err := os.Open(path); err != nil {
		return nil, FileNopCloser, err
	} else {
		closer := func() error { return f.Close() }
		return f, closer, nil
	}
}

func FileSize(f *os.File) (int, error) {
	var size int
	info, err := f.Stat()
	if err != nil {
		return 0, errors.Wrap(err, "could not measure file size")
	}

	size64 := info.Size()
	if int64(int(size64)) == size64 {
		size = int(size64)
	}

	return size, nil
}
