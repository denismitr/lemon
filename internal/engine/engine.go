package engine

import (
	"encoding/json"
	"github.com/denismitr/lemon/internal/data"
	"github.com/denismitr/lemon/internal/storage"
	"github.com/pkg/errors"
)

var ErrDocumentNotFound = errors.New("document not found")
var ErrKeyAlreadyExists = errors.New("key already exists")

type Engine struct {
	s storage.Storage
	dm data.Model
}

func New(s storage.Storage) *Engine {
	return &Engine{
		s: s,
	}
}

func (e *Engine) Init() error {
	size, err := e.s.Size()
	if err != nil {
		return errors.Wrap(err, "init DB failed")
	}

	if size == 0 {
		e.dm.PKs = make(data.PrimaryKeys)
		e.dm.Documents = make([]data.Document, 0)
		if err := e.s.Write(e.dm); err != nil {
			return errors.Wrap(err, "init DB failed")
		}
	} else {
		if loadErr := e.loadFromStorage(); loadErr != nil {
			return errors.Wrap(loadErr, "init DB failed")
		}
	}

	return nil
}

func (e *Engine) loadFromStorage() error {
	if err := e.s.Read(&e.dm); err != nil {
		return errors.Wrap(err, "could not load data from storage")
	}

	return nil
}

func (e *Engine) FindDocumentByKey(pk string) (*data.Document, error) {
	for k, docIdx := range e.dm.PKs {
		if k == pk {
			if len(e.dm.Documents) < docIdx + 1 {
				return nil, errors.Wrap(ErrDocumentNotFound, "document index not found")
			}

			return &e.dm.Documents[docIdx], nil
		}
	}

	return nil, errors.Wrapf(ErrDocumentNotFound, "search by primary key %s", pk)
}

func (e *Engine) AddDocument(key string, d interface{}) error {
	for pk, _ := range e.dm.PKs {
		if pk == key {
			return errors.Wrapf(ErrKeyAlreadyExists, "%s", key)
		}
	}

	var v string
	if s, isStr := d.(string); isStr {
		v = s
	} else {
		b, err := json.Marshal(d)
		if err != nil {
			return errors.Wrapf(err, "could not marshal data %+v", d)
		}
		v = string(b)
	}

	doc := data.Document{Key: key, Value: v}
	e.dm.Documents = append(e.dm.Documents, doc)
	lastRecordPos := len(e.dm.Documents)
	e.dm.PKs[key] = lastRecordPos - 1

	if err := e.s.Write(e.dm); err != nil {
		return err
	}

	return nil
}