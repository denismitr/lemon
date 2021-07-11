package storage


type BoolTag struct {
	Key   string `json:"k"`
	Value bool   `json:"v"`
}

type FloatTag struct {
	Key   string  `json:"k"`
	Value float64 `json:"v"`
}

type IntTag struct {
	Key   string `json:"k"`
	Value int    `json:"v"`
}

type StrTag struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

type Tags struct {
	BoolTag  []BoolTag  `json:"b"`
	FloatTag []FloatTag `json:"f"`
	IntTag   []IntTag   `json:"i"`
	StrTag   []StrTag   `json:"s"`
}

type TagSetter func(tags *Tags)

func BoolTagSetter(k string, v bool) TagSetter {
	return func(tags *Tags) {
		tags.BoolTag = append(tags.BoolTag, BoolTag{k, v})
	}
}
