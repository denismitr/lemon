package data

type PrimaryKeys []string

type Value string

func (i Value) String() string {
	return string(i)
}

type Model struct {
	PKs    PrimaryKeys `json:"pks"`
	Values []Value     `json:"documents"`
}

type Item struct {
	Key string
	Value Value
}

