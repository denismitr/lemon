package lemon

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_SimpleJsonDocument(t *testing.T) {
	d := &Document{
		key: "user:123",
		value: []byte(`{"str":"foo bar baz","emptyStr":null,"null":null,"float":345.54,"zeroFloat":0,"int":452,"zeroInt":0,"trueBool":true,"falseBool":false}`),
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

	t.Run("str", func(t *testing.T) {
		s, err := js.String("str")
		require.NoError(t, err)
		assert.Equal(t, "foo bar baz", s)

		sz, err := js.String("emptyStr")
		require.NoError(t, err)
		assert.Equal(t, "", sz)

		emp, err := js.String("nonExistent")
		require.Error(t, err)
		assert.Equal(t, "", emp)

		sOrD := js.StringOrDefault("str", "abc")
		assert.Equal(t, "foo bar baz", sOrD)

		szOrD := js.StringOrDefault("emptyStr", "abc")
		assert.Equal(t, "", szOrD)

		def := js.StringOrDefault("nonExistent", "abc")
		assert.Equal(t, "abc", def)
	})

	t.Run("bool", func(t *testing.T) {
		b, err := js.Bool("trueBool")
		require.NoError(t, err)
		assert.Equal(t, true, b)

		fb, err := js.Bool("falseBool")
		require.NoError(t, err)
		assert.Equal(t, false, fb)

		emp, err := js.Bool("nonExistent")
		require.Error(t, err)
		assert.Equal(t, false, emp)

		trueOrFalse := js.BoolOrDefault("trueBool", false)
		assert.Equal(t, true, trueOrFalse)

		fbOrD := js.BoolOrDefault("falseBool", true)
		assert.Equal(t, false, fbOrD)

		def := js.BoolOrDefault("nonExistent", true)
		assert.Equal(t, true, def)
	})
}

func BenchmarkDocument_JSON(b *testing.B) {
	d := &Document{
		key: "user:123",
		value: []byte(`{"str":"foo bar baz","emptyStr":null,"null":null,"float":345.54,"zeroFloat":0,"int":452,"zeroInt":0,"trueBool":true,"falseBool":false}`),
	}

	js := d.JSON()

	for i := 0; i < b.N; i++ {
		js.Float("float")
		js.Float("zeroFloat")
		js.Float("nonExistent")
		js.FloatOrDefault("float", 44)
		js.FloatOrDefault("zeroFloat", 77)
		js.FloatOrDefault("nonExistent", 44.9)

		js.Int("int")
		js.Int("zeroInt")
		js.Int("nonExistent")
		js.IntOrDefault("int", 44)
		js.IntOrDefault("zeroInt", 77)
		js.IntOrDefault("nonExistent", 44)
	}
}
