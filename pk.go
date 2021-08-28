package lemon

import (
	"strconv"
	"strings"
)

type PK struct {
	key string
	segments []string
}

func newPK(k string) PK {
	return PK {
		key: k,
		segments: strings.Split(k, ":"),
	}
}

func (pk *PK) Match(patterns []string) bool {
	if len(patterns) == 0 || (len(patterns) == 1 && patterns[0] == "*") {
		return true
	}

	for i := 0; i < len(patterns); i++ {
		if i > len(pk.segments) - 1 {
			return patterns[i] == "*"
		}

		if patterns[i] != pk.segments[i] && patterns[i] != "*" {
			return false
		}
	}

	return true
}

func (pk *PK) Equal(other *PK) bool {
	return pk.key == other.key
}

func (pk *PK) String() string {
	return pk.key
}

func (pk *PK) Less(other PK) bool {
	l := smallestSegmentLen(pk.segments, other.segments)

	prevEq := false
	for i := 0; i < l; i++ {
		// try to compare as ints
		bothInts, a, b := convertToINTs(pk.segments[i], other.segments[i])
		if bothInts {
			if a != b {
				return a < b
			}

			prevEq = true
			continue
		}

		// try to compare as strings
		if pk.segments[i] != other.segments[i]  {
			return pk.segments[i] < other.segments[i]
		}

		prevEq = pk.segments[i] == other.segments[i]
	}

	return prevEq && len(other.segments) > len(pk.segments)
}

func byPrimaryKeys(a, b interface{}) bool {
	i1, i2 := a.(*entry), b.(*entry)
	return i1.key.Less(i2.key)
}

func smallestSegmentLen(a, b []string) int {
	if len(a) > len(b) {
		return len(b)
	}

	return len(a)
}

func convertToINTs(a, b string) (bool, int, int) {
	if a == "" || b == "" || a[0] == '0' || b[0] == '0' {
		return false, 0, 0
	}

	an, err := strconv.Atoi(a)
	if err != nil {
		return false, 0, 0
	}

	bn, err := strconv.Atoi(b)
	if err != nil {
		return false, 0, 0
	}

	return true, an, bn
}
