package lemon

import "sort"

type stringIndex map[string]map[string][]int

func (si stringIndex) add(tagName, v string, offset int) {
	if si[tagName] == nil {
		si[tagName] = make(map[string][]int)
	}

	si[tagName][v] = append(si[tagName][v], offset)
	sort.Ints(si[tagName][v])
}

func (si stringIndex) findOffsets(k, v string) []int {
	offsets := si[k][v]
	if offsets == nil {
		offsets = []int{}
	}
	return offsets
}

func (si stringIndex) removeOffset(tagName, v string, offset int) bool {
	if si[tagName] == nil {
		return false
	}

	for i, o := range si[tagName][v] {
		if o == offset {
			si[tagName][v] = append(si[tagName][v][:i], si[tagName][v][i+1:]...)
			return true
		}
	}

	return false
}

func (si stringIndex) removeOffsetAndShift(tagName, v string, offset int) bool {
	if si[tagName] == nil {
		return false
	}

	found := false
	for i, o := range si[tagName][v] {
		if o == offset {
			si[tagName][v] = append(si[tagName][v][:i], si[tagName][v][i+1:]...)
			found = true
		} else if found {
			// if found we decrement all following offsets because document was removed at offset
			si[tagName][v][i-1]--
		}
	}

	return found
}

type boolIndex map[string]map[bool][]int

func (bi boolIndex) add(tagName string, v bool, offset int) {
	if bi[tagName] == nil {
		bi[tagName] = make(map[bool][]int)
	}

	bi[tagName][v] = append(bi[tagName][v], offset)
	sort.Ints(bi[tagName][v])
}

func (bi boolIndex) findOffsets(k string, v bool) []int {
	return bi[k][v]
}

func (bi boolIndex) removeOffset(tagName string, v bool, offset int) bool {
	if bi[tagName] == nil {
		return false
	}

	for i, o := range bi[tagName][v] {
		if o == offset {
			bi[tagName][v] = append(bi[tagName][v][:i], bi[tagName][v][i+1:]...)
			return true
		}
	}

	return false
}

func (bi boolIndex) removeOffsetAndShift(tagName string, v bool, offset int) bool {
	if bi[tagName] == nil {
		return false
	}

	found := false
	for i, o := range bi[tagName][v] {
		if o == offset {
			bi[tagName][v] = append(bi[tagName][v][:i], bi[tagName][v][i+1:]...)
			found = true
		} else if found {
			// if found we decrement all following offsets because document was removed at offset
			bi[tagName][v][i-1]--
		}
	}

	return found
}
