package options

type KeyRange struct {
	From, To string
}

type Order string

const (
	Ascend Order = "ASC"
	Descend Order = "DESC"
)

type FindOptions struct {
	O Order
	KR *KeyRange
	Px string
}

func (fo *FindOptions) Order(o Order) *FindOptions {
	fo.O = o
	return fo
}

func (fo *FindOptions) KeyRange(from, to string) *FindOptions {
	fo.KR = &KeyRange{From: from, To: to}
	return fo
}

func (fo *FindOptions) Prefix(p string) *FindOptions {
	fo.Px = p
	return fo
}

func Find() *FindOptions {
	return &FindOptions{O: Ascend}
}

