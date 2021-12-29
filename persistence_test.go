package lemon

import (
	"bufio"
	"bytes"
	"github.com/denismitr/lemon/internal/lru"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func Test_resolveRespArrayFromLine(t *testing.T) {
	tt := []struct{
		in string
		bytesExpected int
		segments int
	}{
		{in: "*5\r\n$3\r\nset\r\n$6\r\nfoo123\r\n$10\r\n+btg(bar,1)\r\n", bytesExpected: 4, segments: 5},
		{in: "*3\r\n$3\r\ndel\r\n$6foo123", bytesExpected: 4, segments: 3},
		{in: "*34\r\n", bytesExpected: 5, segments: 34},
	}

	for _, tc := range tt {
		t.Run("valid array cmd", func(t *testing.T) {
			p := &respParser{}

			b := &bytes.Buffer{}
			b.WriteString(tc.in)
			r := bufio.NewReader(b)

			segments, err := p.resolveRespArrayFromLine(r)
			require.NoError(t, err)
			assert.Equal(t, tc.segments, segments)
			assert.Equal(t, tc.bytesExpected, p.currentCmdSize)
		})
	}
}

type commandsMock struct {
	commands []deserializable
	delCommands int
	setCommands int
}

func (cm *commandsMock) acceptWithSuccess(d deserializable) error {
	cm.commands = append(cm.commands, d)

	if _, ok := d.(*deleteCmd); ok {
		cm.delCommands++
	}

	if _, ok := d.(*entry); ok {
		cm.setCommands++
	}

	return nil
}

func Test_writeRespArray(t *testing.T) {
	tt := []struct{
		segments int
		expected string
	}{
		{segments: 3, expected: "*3\r\n"},
		{segments: 56, expected: "*56\r\n"},
		{segments: 0, expected: "*0\r\n"},
	}

	for _, tc := range tt {
		t.Run(tc.expected, func(t *testing.T) {
			b := &bytes.Buffer{}
			writeRespArray(tc.segments, b)
			assert.Equal(t, tc.expected, b.String())
		})
	}
}

func Test_writeRespBlob(t *testing.T) {
	tt := []struct{
		in []byte
		expected string
	}{
		{in: []byte(`{"foo":"bar","baz":3456,"one":nil}`), expected: "$34\r\n" + `{"foo":"bar","baz":3456,"one":nil}` + "\r\n"},
		{in: []byte(`hello`), expected: "$5\r\nhello\r\n"},
	}

	for _, tc := range tt {
		t.Run(tc.expected, func(t *testing.T) {
			b := &bytes.Buffer{}
			writeRespBlob(tc.in, b)
			assert.Equal(t, tc.expected, b.String())
		})
	}
}

func Test_parser(t *testing.T) {
	t.Run("it can process valid set and del commands without tags", func(t *testing.T) {
		mock := &commandsMock{}
		prs := &respParser{vls: EagerLoad}

		cmds := strings.Join([]string{
			"*3\r\n+set\r\n$8\r\nuser:123\r\n$13\r\n" + `{"foo":"bar"}` + "\r\n",
			"*3\r\n+set\r\n$8\r\nuser:456\r\n$11\r\n" + `{"baz":123}` + "\r\n",
			"*2\r\n+del\r\n$8\r\nuser:123\r\n",
			"*3\r\n+set\r\n$14\r\nproducts/items\r\n$15\r\n" + `[1,4,6,7,8,985]` + "\r\n",
		}, "")

		c, _ := lru.NewCache(20, gb)
		r := bufio.NewReader(strings.NewReader(cmds))
		n, err := prs.parse(r, c, mock.acceptWithSuccess)

		require.NoError(t, err)
		require.Equal(t, len([]byte(cmds)), n)

		require.NotNil(t, mock.commands)
		require.Len(t, mock.commands, 4)
		assert.Equal(t, 3, mock.setCommands)
		assert.Equal(t, 1, mock.delCommands)

		cmd1, ok := mock.commands[0].(*entry)
		require.True(t, ok)
		assert.Equal(t, newPK("user:123"), cmd1.key)
		v1, ok := c.Get(cmd1.pos.offset)
		assert.Equal(t, []byte(`{"foo":"bar"}`), v1)
		assert.True(t, ok)

		cmd2, ok := mock.commands[1].(*entry)
		require.True(t, ok)
		assert.Equal(t, newPK("user:456"), cmd2.key)
		v2, ok := c.Get(cmd2.pos.offset)
		assert.Equal(t, []byte(`{"baz":123}`), v2)
		assert.True(t, ok)
		assert.Nil(t, cmd2.tags)

		cmd3, ok := mock.commands[2].(*deleteCmd)
		require.True(t, ok)
		assert.Equal(t, newPK("user:123"), cmd3.key)

		cmd4, ok := mock.commands[3].(*entry)
		require.True(t, ok)
		assert.Equal(t, newPK("products/items"), cmd4.key)
		v4, ok := c.Get(cmd4.pos.offset)
		assert.Equal(t, []byte(`[1,4,6,7,8,985]`), v4)
		assert.True(t, ok)
		assert.Nil(t, cmd2.tags)
	})

	t.Run("it can process valid set and del commands with tags", func(t *testing.T) {
		mock := &commandsMock{}
		prs := &respParser{vls: EagerLoad}

		cmds := strings.Join([]string{
			"*4\r\n+set\r\n$8\r\nuser:123\r\n$13\r\n" + `{"foo":"bar"}` + "\r\n+stg(bar,one_two_three)\n",
			"*4\r\n+set\r\n$8\r\nuser:456\r\n$11\r\n" + `{"baz":123}` + "\r\n+btg(foo,true)\r\n",
			"*2\r\n+del\r\n$8\r\nuser:123\r\n",
			"*3\r\n+set\r\n$14\r\nproducts/items\r\n$15\r\n" + `[1,4,6,7,8,985]` + "\r\n",
		}, "")

		c, _ := lru.NewCache(valueShards, gb)
		r := bufio.NewReader(strings.NewReader(cmds))
		n, err := prs.parse(r, c, mock.acceptWithSuccess)

		require.NoError(t, err)
		require.Equal(t, len([]byte(cmds)), n)

		require.NotNil(t, mock.commands)
		require.Len(t, mock.commands, 4)
		assert.Equal(t, 3, mock.setCommands)
		assert.Equal(t, 1, mock.delCommands)

		cmd1, ok := mock.commands[0].(*entry)
		require.True(t, ok)
		assert.Equal(t, newPK("user:123"), cmd1.key)

		v1, ok := c.Get(cmd1.pos.offset)
		assert.Equal(t, []byte(`{"foo":"bar"}`), v1)
		assert.True(t, ok)

		require.NotNil(t, cmd1.tags)
		require.Len(t, cmd1.tags, 1)
		require.Equal(t, M{"bar":"one_two_three"}, cmd1.tags.asMap())

		cmd2, ok := mock.commands[1].(*entry)
		require.True(t, ok)
		assert.Equal(t, newPK("user:456"), cmd2.key)
		v2, ok := c.Get(cmd2.pos.offset)
		assert.Equal(t, []byte(`{"baz":123}`), v2)
		assert.True(t, ok)
		require.NotNil(t, cmd2.tags)
		require.Len(t, cmd2.tags, 1)
		require.Equal(t, M{"foo":true}, cmd2.tags.asMap())

		cmd3, ok := mock.commands[2].(*deleteCmd)
		require.True(t, ok)
		assert.Equal(t, newPK("user:123"), cmd3.key)

		cmd4, ok := mock.commands[3].(*entry)
		require.True(t, ok)
		assert.Equal(t, newPK("products/items"), cmd4.key)
		v4, ok := c.Get(cmd4.pos.offset)
		assert.Equal(t, []byte(`[1,4,6,7,8,985]`), v4)
		assert.True(t, ok)
		assert.Nil(t, cmd4.tags)
	})
}
