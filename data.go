package lemon

type record struct {
	PK    string `json:"k"`
	Value []byte `json:"v"`
	Tags  *Tags `json:"t"`
}

// dm - data model
type dm struct {
	Records []record `json:"records"`
}
