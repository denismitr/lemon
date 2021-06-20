package engine

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

	prevEq := true
	for i := 0; i < l; i++ {
		if less, bothInts := tryToCompareAsINTs(ourSegments[i], otherSegments[i]); bothInts && less && prevEq {
			return less
		} else if bothInts {
			continue
		}

		if ourSegments[i] < otherSegments[i] && prevEq {
			return true
		} else if ourSegments[i] == otherSegments[i] {
			// last iteration
			if i == l - 1 {
				return true
			} else {
				prevEq = true
			}
		} else {
			prevEq = false
		}
	}

	return false
}

func smallestSegmentLen(a, b []string) int {
	if len(a) > len(b) {
		return len(b)
	}

	return len(a)
}

func isPositiveInt(s string) bool {
	n, err := strconv.Atoi(s)
	if err != nil {
		return false
	}

	return n > 0
}

func tryToCompareAsINTs(a, b string) (less bool, bothInts bool) {
	an, err := strconv.Atoi(a)
	if err != nil {
		return false, false
	}

	bn, err := strconv.Atoi(b)
	if err != nil {
		return false, false
	}

	return an < bn, true
}

