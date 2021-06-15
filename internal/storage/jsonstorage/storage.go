package jsonstorage

import (
	"encoding/json"
	"github.com/pkg/errors"
	"io"
	"os"
)

func OpenOrCreate(path string) (*os.File, error) {
	var file *os.File
	if _, err := os.Stat(path); os.IsNotExist(err) {
		f, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		file = f
	} else {
		f, err := os.OpenFile(path, os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		file = f
	}

	return file, nil
}

type JSONStorage struct {
	f *os.File
}

func NewJSONStorage(f *os.File) *JSONStorage {
	return &JSONStorage{f: f}
}

func (s *JSONStorage) Size() (int, error) {
	var size int
	info, err := s.f.Stat();
	if err != nil {
		return 0, errors.Wrap(err, "could not measure file size")
	}

	size64 := info.Size()
	if int64(int(size64)) == size64 {
		size = int(size64)
	}

	return size, nil
}

func (s *JSONStorage) Write(data interface{}) error {
	b, err := json.Marshal(data)
	if err != nil {
		return errors.Wrapf(err, "could not marshal data %+v ", data)
	}

	if _, err := s.f.Seek(0, 0); err != nil {
		return errors.Wrapf(err, "could not seek the begging of the file %s", s.f.Name())
	}

	if _, err := s.f.Write(b); err != nil {
		return errors.Wrapf(err, "could not write to file %s", s.f.Name())
	}

	if err := s.f.Sync(); err != nil {
		return errors.Wrapf(err, "could not sync file %s", s.f.Name())
	}

	return nil
}

func (s *JSONStorage) Read(dest interface{}) error {
	if _, err := s.f.Seek(0, 0); err != nil {
		return err
	}

	size, err := s.Size()
	if err != nil {
		return err
	}

	size++ // one byte for final read at EOF

	// If a file claims a small size, read at least 512 bytes.
	// In particular, files in Linux's /proc claim size 0 but
	// then do not work right if read in small pieces,
	// so an initial read of 1 byte would not work correctly.
	if size < 512 {
		size = 512
	}

	data := make([]byte, 0, size)
	for {
		if len(data) >= cap(data) {
			d := append(data[:cap(data)], 0)
			data = d[:len(data)]
		}
		n, err := s.f.Read(data[len(data):cap(data)])
		data = data[:len(data)+n]
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}
	}

	if err := json.Unmarshal(data, &dest); err != nil {
		return errors.Wrapf(err, "could not unmarshal %s", string(data))
	}

	return nil
}

