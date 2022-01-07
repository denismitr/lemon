package lemon_test

import (
	"math/rand"
	"time"
)

var seededString *rand.Rand

func init() {
	seededString = rand.New(rand.NewSource(time.Now().UnixNano()))
	rand.Seed(time.Now().UnixNano())
}

const alphaNumericCharset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = alphaNumericCharset[seededString.Intn(len(alphaNumericCharset))]
	}
	return string(b)
}

func RandomBoolString(a, b string) string {
	if rand.Int() % 2 == 0 {
		return a
	}
	return b
}


