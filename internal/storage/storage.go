package storage

type Storage interface {
	Write(data interface{}) error
	Read(dest interface{}) error
	Size() (int, error)
}
