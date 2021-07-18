package lemon

type D map[string]interface{}

type KeyRange struct {
	From, To string
}

type Order string

const (
	Ascend  Order = "ASC"
	Descend Order = "DESC"
)

type queryTags struct {
	boolTags []boolTag
	strTags  []strTag
}

type queryOptions struct {
	order    Order
	keyRange *KeyRange
	prefix   string
	tags     *queryTags
}

func (fo *queryOptions) Order(o Order) *queryOptions {
	fo.order = o
	return fo
}

func (fo *queryOptions) KeyRange(from, to string) *queryOptions {
	fo.keyRange = &KeyRange{From: from, To: to}
	return fo
}

func (fo *queryOptions) Prefix(p string) *queryOptions {
	fo.prefix = p
	return fo
}

func (fo *queryOptions) BoolTag(name string, v bool) *queryOptions {
	if fo.tags == nil {
		fo.tags = &queryTags{}
	}

	fo.tags.boolTags = append(fo.tags.boolTags, boolTag{Name: name, Value: v})
	return fo
}

func Q() *queryOptions {
	return &queryOptions{order: Ascend}
}
