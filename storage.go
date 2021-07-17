package lemon

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

type jsonStorage struct {
	fullPath string
	tmpPath  string

	mu sync.RWMutex

	dm dm
}

func newJsonStorage(fullPath string) *jsonStorage {
	if !strings.HasSuffix(fullPath, ".ldb") {
		fullPath += ".ldb"
	}

	tmpPath := strings.TrimSuffix(fullPath, ".ldb") + ".tmp"

	return &jsonStorage{fullPath: fullPath, tmpPath: tmpPath}
}

func (s *jsonStorage) records() []record {
	return s.dm.Records
}

func (s *jsonStorage) iterate(f func(o int, k string, v []byte, t *Tags)) {
	for i := range s.dm.Records {
		f(i, s.dm.Records[i].PK, s.dm.Records[i].Value, s.dm.Records[i].Tags)
	}
}

func (s *jsonStorage) len() int {
	return len(s.dm.Records)
}

func (s *jsonStorage) lastOffset() int {
	offset := len(s.dm.Records) - 1
	if offset < 0 {
		return 0
	}
	return offset
}

func (s *jsonStorage) nextOffset() int {
	return len(s.dm.Records)
}

func (s *jsonStorage) append(k string, v []byte, ts ...TagSetter) int {
	r := record{
		PK: k,
		Value: v,
	}

	tags := Tags{}
	for _, s := range ts {
		s(&tags)
	}

	r.Tags = &tags
	s.dm.Records = append(s.dm.Records, r)

	return len(s.dm.Records) - 1
}

func (s *jsonStorage) replaceValueAt(offset int, v []byte, ts ...TagSetter) error {
	if len(s.dm.Records) < offset+1 {
		return errors.Errorf("offset %d is out of range for values", offset)
	}

	tags := Tags{}
	for _, s := range ts {
		s(&tags)
	}

	s.dm.Records[offset].Tags = &tags
	s.dm.Records[offset].Value = v
	return nil
}

func (s *jsonStorage) getValueAt(offset int) ([]byte, error) {
	if offset < 0 {
		panic("offset cannot be less than 0")
	}

	if len(s.dm.Records) < offset+1 {
		return nil, errors.Errorf("offset %d is out of range for values", offset)
	}

	return s.dm.Records[offset].Value, nil
}

func (s *jsonStorage) removeAt(offset int) error {
	if offset < 0 {
		panic("how can offset be less than 0?")
	}

	if len(s.dm.Records) < offset+1 {
		return errors.Errorf("offset %d is out of range for records", offset)
	}

	s.dm.Records = append(s.dm.Records[:offset], s.dm.Records[offset+1:]...)
	return nil
}

func (s *jsonStorage) initialize() error {
	if s.dm.Records == nil {
		s.dm.Records = make([]record, 0)
		return s.write()
	}

	return nil
}

func (s *jsonStorage) persist() error {
	if s.dm.Records == nil {
		return s.initialize()
	}

	return s.write()
}

func (s *jsonStorage) load() error {
	if !storage.FileExists(s.fullPath) {
		return s.initialize()
	}

	return s.read()
}

func (s *jsonStorage) write() error {
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

func (s *jsonStorage) read() error {
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
