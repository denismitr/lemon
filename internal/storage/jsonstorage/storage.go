package jsonstorage

import (
	"bytes"
	"encoding/json"
	"github.com/denismitr/lemon/internal/storage"
	"github.com/pkg/errors"
	"io"
	"os"
	"strings"
	"sync"
)

type model struct {
	PKs    []string `json:"pks"`
	Values [][]byte `json:"documents"`
}

type JSONStorage struct {
	fullPath string
	tmpPath string

	mu sync.RWMutex

	dm model
}

func New(fullPath string) *JSONStorage {
	if !strings.HasSuffix(fullPath, ".ldb") {
		fullPath += ".ldb"
	}

	tmpPath := strings.TrimSuffix(fullPath, ".ldb") + ".tmp"

	return &JSONStorage{fullPath: fullPath, tmpPath: tmpPath}
}

func (s *JSONStorage) PKs() []string {
	return s.dm.PKs
}

func (s *JSONStorage) Len() int {
	if len(s.dm.PKs) != len(s.dm.Values) {
		panic("how can number of pks and number of values not be equal?")
	}

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
	if !storage.FileExists(s.fullPath) {
		return s.Initialize()
	}

	return s.read()
}

func (s *JSONStorage) write() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tmpF, tmpClose, err := storage.CreateFileUnderLock(s.tmpPath, storage.DefaultFilePerm)
	if err != nil {
		return err
	}

	e := json.NewEncoder(tmpF)
	if err := e.Encode(&s.dm); err != nil {
		tmpClose() // log
		os.Remove(tmpF.Name())
		return errors.Wrapf(err, "could not write to tmp file %s", tmpF.Name())
	}

	if err := tmpF.Sync(); err != nil {
		tmpClose() // log
		os.Remove(tmpF.Name())
		return errors.Wrapf(err, "could not sync tmp file %s", tmpF.Name())
	}

	if err := tmpClose(); err != nil {
		return errors.Wrapf(err, "could not close tmp file %s", tmpF.Name())
	}

	if err := os.Rename(tmpF.Name(), s.fullPath); err != nil {
		os.Remove(tmpF.Name())
		return errors.Wrapf(err, "could not replace %s with %s", s.fullPath, tmpF.Name())
	}

	return nil
}

func (s *JSONStorage) read() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	f, fClose, err := storage.OpenFile(s.fullPath)
	if err != nil {
		return err
	}

	defer fClose()

	size, err := storage.FileSize(f)
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
		n, err := f.Read(data[len(data):cap(data)])
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
