package engine

// dm - data model
type dm struct {
	PKs    []string `json:"pks"`
	Tags   []Tags   `json:"tags"`
	Values [][]byte `json:"documents"`
}
