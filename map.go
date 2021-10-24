package lemon

import "github.com/pkg/errors"

type M map[string]interface{}

func (m M) applyTo(e *entry) error {
	for k, v := range m {
		switch typedValue := v.(type) {
		case int:
			if it, ok := e.tags.names[k]; ok && it != intDataType {
				return errors.Wrapf(ErrInvalidTagType, "key %s already taken and has another type", k)
			}

			e.tags.integers[k] = typedValue
			e.tags.names[k] = intDataType
		case string:
			if it, ok := e.tags.names[k]; ok && it != strDataType {
				return errors.Wrapf(ErrInvalidTagType, "key %s already taken and has another type", k)
			}

			e.tags.strings[k] = typedValue
			e.tags.names[k] = strDataType
		case float64:
			if it, ok := e.tags.names[k]; ok && it != strDataType {
				return errors.Wrapf(ErrInvalidTagType, "key %s already taken and has another type", k)
			}

			e.tags.floats[k] = typedValue
			e.tags.names[k] = floatDataType
		case bool:
			if it, ok := e.tags.names[k]; ok && it != strDataType {
				return errors.Wrapf(ErrInvalidTagType, "key %s already taken and has another type", k)
			}

			e.tags.booleans[k] = typedValue
			e.tags.names[k] = boolDataType
		default:
			return errors.Wrapf(ErrInvalidTagType, "key %s has unsupported type for LemonDB tags", k)
		}
	}

	return nil
}

func (m M) String(k string) string {
	v, ok := m[k].(string)
	if !ok {
		return ""
	}
	return v
}

func (m M) HasString(k string) bool {
	_, ok := m[k].(string)
	return ok
}

func (m M) Int(k string) int {
	v, ok := m[k].(int)
	if !ok {
		return 0
	}
	return v
}

func (m M) HasInt(k string) bool {
	_, ok := m[k].(int)
	return ok
}

func (m M) Bool(k string) bool {
	v, ok := m[k].(bool)
	if !ok {
		return false
	}
	return v
}

func (m M) HasBool(k string) bool {
	_, ok := m[k].(bool)
	return ok
}

func (m M) Float(k string) float64 {
	v, ok := m[k].(float64)
	if !ok {
		return 0
	}
	return v
}

func (m M) HasFloat(k string) bool {
	_, ok := m[k].(float64)
	return ok
}
