package lemon

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSettingTagValues(t *testing.T) {
	const nameA = "nameA"
	const nameB = "nameB"
	const nameC = "nameC"

	validTags := []struct {
		valueA interface{}
		valueB interface{}
		valueC interface{}
		exp    M
	}{
		{
			valueA: 123,
			valueB: "foo",
			valueC: 345.33,
			exp:    M{nameA: 123, nameB: "foo", nameC: 345.33},
		},
		{
			valueA: true,
			valueB: "",
			valueC: -9999,
			exp:    M{nameA: true, nameB: "", nameC: -9999},
		},
	}

	for i, tc := range validTags {
		t.Run(fmt.Sprintf("Valid values test case: %d", i), func(t *testing.T) {
			tgs := newTags()
			require.NoError(t, tgs.set(nameA, tc.valueA))
			require.NoError(t, tgs.set(nameB, tc.valueB))
			require.NoError(t, tgs.set(nameC, tc.valueC))

			assert.Exactly(t, tc.exp, tgs.asMap())
		})
	}

	invalidTags := []struct {
		value interface{}
	}{
		{value: int64(455)},
		{value: int8(2)},
		{value: float32(.2)},
	}

	for i, tc := range invalidTags {
		t.Run(fmt.Sprintf("Invalid values test case: %d", i), func(t *testing.T) {
			tgs := newTags()
			err := tgs.set(nameA, tc.value)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrInvalidTagType))
		})
	}
}
