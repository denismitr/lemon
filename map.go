package lemon

import "github.com/pkg/errors"

type M map[string]interface{}

func (m M) applyTo(e *entry) error {
	for k, v := range m {
		existingTag := e.tags[k]
		newTag := &tag{data: v}

		if existingTag != nil && k == CreatedAt {
			continue
		}

		switch v.(type) {
		case int:
			if existingTag != nil && existingTag.dt != intDataType {
				return errors.Wrapf(ErrInvalidTagType, "key %s already taken and has another type", k)
			}

			newTag.dt = intDataType
		case string:
			if existingTag != nil && existingTag.dt != strDataType {
				return errors.Wrapf(ErrInvalidTagType, "key %s already taken and has another type", k)
			}

			newTag.dt = strDataType
		case float64:
			if existingTag != nil && existingTag.dt != floatDataType {
				return errors.Wrapf(ErrInvalidTagType, "key %s already taken and has another type", k)
			}

			newTag.dt = floatDataType
		case bool:
			if existingTag != nil && existingTag.dt != boolDataType {
				return errors.Wrapf(ErrInvalidTagType, "key %s already taken and has another type", k)
			}

			newTag.dt = boolDataType
		default:
			return errors.Wrapf(ErrInvalidTagType, "key %s has unsupported type for LemonDB tags", k)
		}

		e.tags[k] = newTag
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
