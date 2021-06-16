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
		e.dm.Values = make([]data.Value, 0)
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

func (e *Engine) Persist() error {
	if e.dm.PKs == nil {
		panic("how could primaryKeys map be empty?")
	}

	if e.dm.Values == nil {
		panic("how could values slice be empty?")
	}

	if err := e.s.Write(e.dm); err != nil {
		if loadErr := e.loadFromStorage(); loadErr != nil {
			return errors.Wrap(loadErr, "persist DB failed and could not reload from disk")
		}

		return errors.Wrap(err, "persist DB failed")
	}

	if loadErr := e.loadFromStorage(); loadErr != nil {
		return errors.Wrap(loadErr, "persist DB failed and could not reload from disk")
	}

	return nil
}

func (e *Engine) loadFromStorage() error {
	if err := e.s.Read(&e.dm); err != nil {
		return errors.Wrap(err, "could not load data from storage")
	}

	return nil
}

func (e *Engine) FindByKey(pk string) (*data.Value, error) {
	for k, docIdx := range e.dm.PKs {
		if k == pk {
			if len(e.dm.Values) < docIdx + 1 {
				return nil, errors.Wrap(ErrDocumentNotFound, "document index not found")
			}

			return &e.dm.Values[docIdx], nil
		}
	}

	return nil, errors.Wrapf(ErrDocumentNotFound, "search by primary key %s", pk)
}

func (e *Engine) RemoveByKeys(pks ...string) error {
	for _, pk := range pks {
		if err := e.removeByKeyFromDataModel(pk); err != nil {
			if loadErr := e.loadFromStorage(); loadErr != nil {
				return errors.Wrap(err, loadErr.Error())
			}
			return err
		}
	}

	if err := e.Persist(); err != nil {
		return err
	}

	return nil
}

func (e *Engine) removeByKeyFromDataModel(key string) error {
	var found bool
	var removedDocumentOffset int
	for pk, offset := range e.dm.PKs {
		if pk == key {
			e.dm.Values = append(e.dm.Values[:offset], e.dm.Values[offset+1:]...)
			found = true
			removedDocumentOffset = offset
			break
		}
	}

	if !found {
		return errors.Wrapf(ErrKeyAlreadyExists, "%s", key)
	}

	delete(e.dm.PKs, key)

	for pk, offset := range e.dm.PKs {
		if offset > removedDocumentOffset {
			e.dm.PKs[pk] = offset - 1
		}
	}

	return nil
}

func (e *Engine) Add(key string, d interface{}) error {
	if _, found := e.dm.PKs[key]; found {
		return errors.Wrapf(ErrKeyAlreadyExists, "%s", key)
	}

	v, err := serializeToValue(d)
	if err != nil {
		return err
	}

	e.dm.Values = append(e.dm.Values, v)
	lastRecordPos := len(e.dm.Values)
	e.dm.PKs[key] = lastRecordPos - 1

	if err := e.Persist(); err != nil {
		return err
	}

	return nil
}

func (e *Engine) Replace(key string, d interface{}) error {
	 offset, found := e.dm.PKs[key]
	 if !found {
		return errors.Wrapf(ErrDocumentNotFound, "%s", key)
	}

	v, err := serializeToValue(d)
	if err != nil {
		return err
	}

	e.dm.Values[offset] = v
	if err := e.Persist(); err != nil {
		return err
	}

	return nil
}

func serializeToValue(d interface{}) (data.Value, error) {
	var v string
	if s, isStr := d.(string); isStr {
		v = s
	} else {
		b, err := json.Marshal(d)
		if err != nil {
			return "", errors.Wrapf(err, "could not marshal data %+v", d)
		}
		v = string(b)
	}

	return data.Value(v), nil
}