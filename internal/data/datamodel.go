package data

type PrimaryKeys map[string]int

type Document struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

type Model struct {
	PKs       PrimaryKeys `json:"pks"`
	Documents []Document  `json:"documents"`
}
