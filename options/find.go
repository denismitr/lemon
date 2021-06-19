package options

type KeyRange struct {
	Lower, Upper string
}

type Order string

const (
	Ascend Order = "ASC"
	Descend Order = "DESC"
)

type FindOptions struct {
	O Order
	KR *KeyRange
}

func (fo *FindOptions) SetOrder(o Order) *FindOptions {
	fo.O = o
	return fo
}

func (fo *FindOptions) KeyRange(lower, upper string) *FindOptions {
	fo.KR = &KeyRange{Lower: lower, Upper: upper}
	return fo
}

func Find() *FindOptions {
	return &FindOptions{O: Ascend}
}

