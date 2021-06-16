package data

type PrimaryKeys map[string]int

type Value string

func (i Value) String() string {
	return string(i)
}

type Model struct {
	PKs    PrimaryKeys `json:"pks"`
	Values []Value     `json:"documents"`
}
