package storage

type Storage interface {
	PKs() []string
	GetValueAt(offset int) ([]byte, error)
	RemoveAt(offset int) error
	ReplaceValueAt(offset int, v []byte) error
	Append(k string, v []byte)
	Initialize() error
	Persist() error
	Load() error
	Len() int
	LastOffset() int
}

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
