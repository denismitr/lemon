package lemondb

import (
	"github.com/denismitr/lemon/internal/storage"
)

type Config struct {
	Storage storage.Storage
}
