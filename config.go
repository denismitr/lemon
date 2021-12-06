package lemon

import "time"

const defaultAutoVacuumMinSize uint64 = 1000

var defaultAutovacuumIntervals = 10 * time.Minute
var defaultPersistenceIntervals = 1 * time.Second

type Config struct {
	PersistenceStrategy       PersistenceStrategy
	ValueLoadStrategy         ValueLoadStrategy
	TruncateFileWhenOpen      bool
	AsyncPersistenceIntervals time.Duration
	DisableAutoVacuum         bool
	AutoVacuumOnlyOnClose     bool
	AutoVacuumMinSize         uint64
	Log                       bool
	AutoVacuumIntervals       time.Duration
}

type EngineOptions interface {
	applyTo(e executionEngine) error
}

func (cfg *Config) applyTo(ee executionEngine) error {
	if cfg.PersistenceStrategy == "" {
		cfg.PersistenceStrategy = Sync
	} else if cfg.PersistenceStrategy == Async && cfg.AsyncPersistenceIntervals == 0 {
		cfg.AsyncPersistenceIntervals = defaultPersistenceIntervals
	}

	if cfg.ValueLoadStrategy != "" {
		cfg.ValueLoadStrategy = EagerLoad
	}

	if cfg.AutoVacuumIntervals == 0 {
		cfg.AutoVacuumIntervals = defaultAutovacuumIntervals
	}

	if cfg.AutoVacuumMinSize == 0 {
		cfg.AutoVacuumMinSize = defaultAutoVacuumMinSize
	}

	ee.SetCfg(cfg)

	return nil
}
