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

type EngineOptions interface {
	applyTo(e *engine) error
}

func (cfg *Config) applyTo(e *engine) error {
	if cfg.PersistenceStrategy == "" {
		cfg.PersistenceStrategy = Sync
	}

	if cfg.AutoVacuumIntervals == 0 {
		cfg.AutoVacuumIntervals = defaultAutovacuumIntervals
	}

	if cfg.AutoVacuumMinSize == 0 {
		cfg.AutoVacuumMinSize = defaultAutoVacuumMinSize
	}

	e.cfg = cfg

	return nil
}