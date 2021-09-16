package lemon

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_SimpleJsonDocument(t *testing.T) {
	d := &Document{
		key: "user:123",
		value: []byte(`{"str":"bar","emptyStr":null,"null":null,"float":345.54,"zeroFloat":0,"int":452,"zeroInt":0,"trueBool":true,"falseBool":false}`),
	}

	js := d.JSON()
	require.NotNil(t, js)

	t.Run("floats", func(t *testing.T) {
		f, err := js.Float("float")
		require.NoError(t, err)
		assert.Equal(t, 345.54, f)

		fz, err := js.Float("zeroFloat")
		require.NoError(t, err)
		assert.Equal(t, float64(0), fz)

		emp, err := js.Float("nonExistent")
		require.Error(t, err)
		assert.Equal(t, float64(0), emp)

		fOrD := js.FloatOrDefault("float", 44)
		assert.Equal(t, 345.54, fOrD)

		fzOrD := js.FloatOrDefault("zeroFloat", 77)
		assert.Equal(t, float64(0), fzOrD)

		def := js.FloatOrDefault("nonExistent", 44.9)
		assert.Equal(t, 44.9, def)
	})

	t.Run("int", func(t *testing.T) {
		i, err := js.Int("int")
		require.NoError(t, err)
		assert.Equal(t, 452, i)

		iz, err := js.Int("zeroInt")
		require.NoError(t, err)
		assert.Equal(t, 0, iz)

		emp, err := js.Int("nonExistent")
		require.Error(t, err)
		assert.Equal(t, 0, emp)

		fOrD := js.IntOrDefault("int", 44)
		assert.Equal(t, 452, fOrD)

		izOrD := js.IntOrDefault("zeroInt", 77)
		assert.Equal(t, 0, izOrD)

		def := js.IntOrDefault("nonExistent", 44)
		assert.Equal(t, 44, def)
	})
}
