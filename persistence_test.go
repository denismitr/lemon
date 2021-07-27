package lemon

import (
	"bufio"
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_resolveRespArrayFromLine(t *testing.T) {
	tt := []struct{
		in string
		bytesExpected int
		segments int
	}{
		{in: "*5\r\n$3\r\nset\r\n$6\r\nfoo123\r\n$10\r\nbtg(bar,1)\r\n", bytesExpected: 4, segments: 5},
		{in: "*3\r\n$3\r\ndel\r\n$6foo123", bytesExpected: 4, segments: 3},
		{in: "*34\r\n", bytesExpected: 5, segments: 34},
	}
	cmdBuf := [1024]byte{}

	for _, tc := range tt {
		t.Run("valid array cmd", func(t *testing.T) {
			b := bytes.Buffer{}
			b.WriteString(tc.in)

			in, err := b.ReadBytes('\n')
			if err != nil {
				t.Fatal(err)
			}

			segments, bytesLen, err := resolveRespArrayFromLine(cmdBuf[:0], in)
			require.NoError(t, err)
			assert.Equal(t, tc.segments, segments)
			assert.Equal(t, tc.bytesExpected, bytesLen)
		})
	}
}

func Test_resolveRespSimpleString(t *testing.T) {
	tt := []struct{
		in string
		bytesExpected int
		expected string
	}{
		{in: "+6\r\nfoo123\r\n", bytesExpected: 8, expected: "foo123"},
	}

	for _, tc := range tt {
		t.Run(tc.in, func(t *testing.T) {
			b := &bytes.Buffer{}
			b.WriteString(tc.in)
			r := bufio.NewReader(b)

			result, bytesLen, err := resolveRespSimpleString(r)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
			assert.Equal(t, tc.bytesExpected, bytesLen)
		})
	}

}
