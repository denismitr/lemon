package lemon

type D map[string]interface{}

type KeyRange struct {
	From, To string
}

type Order string

const (
	Ascend Order = "ASC"
	Descend Order = "DESC"
)

type QueryOptions struct {
	O Order
	KR *KeyRange
	Px string
}

func (fo *QueryOptions) Order(o Order) *QueryOptions {
	fo.O = o
	return fo
}

func (fo *QueryOptions) KeyRange(from, to string) *QueryOptions {
	fo.KR = &KeyRange{From: from, To: to}
	return fo
}

func (fo *QueryOptions) Prefix(p string) *QueryOptions {
	fo.Px = p
	return fo
}

func Q() *QueryOptions {
	return &QueryOptions{O: Ascend}
}
