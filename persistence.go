package lemon

import (
	"bytes"
	"github.com/pkg/errors"

	"os"
)

var ErrDbFileWriteFailed = errors.New("database write failed")

type persistenceStrategy string

const (
	Async persistenceStrategy = "async"
	Sync persistenceStrategy = "sync"

)

type persistence struct {
	strategy persistenceStrategy
	f *os.File
	flushes int
}

func (p *persistence) write(buf bytes.Buffer) error {
	n, err := p.f.Write(buf.Bytes())
	if err != nil {
		if n > 0 {
			// partial write occurred, must rollback the file
			pos, seekErr := p.f.Seek(-int64(n), 1)
			if seekErr != nil {
				panic(seekErr)
			}

			if err := p.f.Truncate(pos); err != nil {
				panic(err)
			}
		}

		_ = p.f.Sync()
		return errors.Wrap(ErrDbFileWriteFailed, err.Error())
	}

	if p.strategy == Sync {
		_ = p.f.Sync()
	}

	p.flushes++
	return nil
}
