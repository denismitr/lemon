package lemon

import (
	"github.com/google/btree"
	"strconv"
	"strings"
)

type index struct {
	key string
	offset int
}

func (p *index) Less(than btree.Item) bool {
	other := than.(*index)
	ourSegments := strings.Split(p.key, ":")
	otherSegments := strings.Split(other.key, ":")
	l := smallestSegmentLen(ourSegments, otherSegments)

	prevEq := false
	for i := 0; i < l; i++ {
		// try to compare as ints
		bothInts, a, b := convertToINTs(ourSegments[i], otherSegments[i])
		if bothInts {
			if a != b {
				return a < b
			} else {
				prevEq = true
				continue
			}
		}

		// try to compare as strings
		if ourSegments[i] != otherSegments[i]  {
			return ourSegments[i] < otherSegments[i]
		} else {
			prevEq = ourSegments[i] == otherSegments[i]
		}
	}

	return prevEq && len(otherSegments) > len(ourSegments)
}

func smallestSegmentLen(a, b []string) int {
	if len(a) > len(b) {
		return len(b)
	}

	return len(a)
}

func convertToINTs(a, b string) (bool, int, int) {
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
