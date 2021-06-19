package jsonstorage

import (
	"bytes"
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

type model struct {
	PKs    []string `json:"pks"`
	Values [][]byte `json:"documents"`
}

type JSONStorage struct {
	f  *os.File
	dm model
}

func NewJSONStorage(f *os.File) *JSONStorage {
	return &JSONStorage{f: f}
}

func (s *JSONStorage) PKs() []string {
	return s.dm.PKs
}

func (s *JSONStorage) Len() int {
	return len(s.dm.PKs)
}

func (s *JSONStorage) LastOffset() int {
	offset := len(s.dm.PKs) - 1
	if offset < 0 {
		return 0
	}
	return offset
}

func (s *JSONStorage) Append(k string, v []byte) {
	s.dm.PKs = append(s.dm.PKs, k)
	s.dm.Values = append(s.dm.Values, v)
}

func (s *JSONStorage) ReplaceValueAt(offset int, v []byte) error {
	if len(s.dm.Values) < offset + 1 {
		return errors.Errorf("offset %d is out of range for values", offset)
	}

	s.dm.Values[offset] = v
	return nil
}

func (s *JSONStorage) GetValueAt(offset int) ([]byte, error) {
	if offset < 0 {
		panic("offset cannot be less than 0")
	}

	if len(s.dm.Values) < offset + 1 {
		return nil, errors.Errorf("offset %d is out of range for values", offset)
	}

	return s.dm.Values[offset], nil
}

func (s *JSONStorage) RemoveAt(offset int) error {
	if len(s.dm.PKs) < offset + 1 {
		return errors.Errorf("offset %d is out of range for primary keys", offset)
	}

	if len(s.dm.Values) < offset + 1 {
		return errors.Errorf("offset %d is out of range for values", offset)
	}

	s.dm.Values = append(s.dm.Values[:offset], s.dm.Values[offset+1:]...)
	s.dm.PKs = append(s.dm.PKs[:offset], s.dm.PKs[offset+1:]...)

	return nil
}

func (s *JSONStorage) Size() (int, error) {
	var size int
	info, err := s.f.Stat()
	if err != nil {
		return 0, errors.Wrap(err, "could not measure file size")
	}

	size64 := info.Size()
	if int64(int(size64)) == size64 {
		size = int(size64)
	}

	return size, nil
}

func (s *JSONStorage) Initialize() error {
	if s.dm.PKs == nil || s.dm.Values == nil {
		s.dm.PKs = []string{}
		s.dm.Values = make([][]byte, 0)
		return s.write()
	}

	return nil
}

func (s *JSONStorage) Persist() error {
	if s.dm.PKs == nil || s.dm.Values == nil {
		return s.Initialize()
	}

	return s.write()
}

func (s *JSONStorage) Load() error {
	if n, err := s.Size(); err != nil {
		return err
	} else if n == 0 {
		return s.Initialize()
	}

	return s.read()
}

func (s *JSONStorage) write() error {
	// fixme: maybe write to tmp file and then replace existing file
	if err := s.f.Truncate(0); err != nil {
		return errors.Wrapf(err, "could not truncate file %s", s.f.Name())
	}

	if err := s.f.Sync(); err != nil {
		return errors.Wrapf(err, "could not sync file %s", s.f.Name())
	}

	if _, err := s.f.Seek(0, 0); err != nil {
		return errors.Wrapf(err, "could not seek the begging of the file %s", s.f.Name())
	}

	e := json.NewEncoder(s.f)
	if err := e.Encode(&s.dm); err != nil {
		return errors.Wrapf(err, "could not write to file %s", s.f.Name())
	}

	if err := s.f.Sync(); err != nil {
		return errors.Wrapf(err, "could not sync file %s", s.f.Name())
	}

	return nil
}

func (s *JSONStorage) read() error {
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

	d := json.NewDecoder(bytes.NewReader(data))
	if err := d.Decode(&s.dm); err != nil {
		return errors.Wrapf(err, "could not unmarshal %s", string(data))
	}

	return nil
}
