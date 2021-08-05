package lemon

import "time"

const defaultAutoVacuumMinSize uint64 = 1000

var defaultAutovacuumIntervals = 10 * time.Minute

type Config struct {
	PersistenceStrategy PersistenceStrategy
	DisableAutoVacuum bool
	AutoVacuumOnlyOnClose bool
	AutoVacuumMinSize uint64
	AutoVacuumIntervals time.Duration
}

type EngineOptions func(e *engine)

func WithConfig(cfg Config) EngineOptions {
	return func(e *engine) {
		e.cfg = cfg
	}
}