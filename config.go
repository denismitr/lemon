package lemon

import (
	"github.com/pkg/errors"
	"time"
)

var ErrInvalidConfiguration = errors.New("invalid configuration")

const defaultAutoVacuumMinSize uint64 = 1000

var defaultAutovacuumIntervals = 10 * time.Minute
var defaultPersistenceIntervals = 1 * time.Second

type Config struct {
	PersistenceStrategy          PersistenceStrategy
	ValueLoadStrategy            ValueLoadStrategy
	TruncateFileWhenOpen         bool
	AsyncPersistenceIntervals    time.Duration
	DisableAutoVacuum            bool
	AutoVacuumOnlyOnCloseOrFlush bool
	AutoVacuumMinSize            uint64
	Log                          bool
	AutoVacuumIntervals          time.Duration
	MaxCacheSize                 uint64
	OnCacheEvict                 OnCacheEvict
}

type EngineOptions interface {
	applyTo(inMemoryOnly bool, e executionEngine) error
}

func (cfg *Config) applyTo(inMemoryOnly bool, ee executionEngine) error {
	if inMemoryOnly {
		return cfg.ensureInMemoryConfiguration(ee)
	}

	if cfg.PersistenceStrategy == "" {
		cfg.PersistenceStrategy = Async
	}

	if cfg.PersistenceStrategy == Async && cfg.AsyncPersistenceIntervals == 0 {
		cfg.AsyncPersistenceIntervals = defaultPersistenceIntervals
	}

	if cfg.ValueLoadStrategy == "" {
		cfg.ValueLoadStrategy = EagerLoad
	}

	if cfg.ValueLoadStrategy == LazyLoad && cfg.MaxCacheSize > 0 {
		return errors.Wrap(ErrInvalidConfiguration, "MaxCacheSize cannot be combined with EagerLoad")
	}

	if cfg.ValueLoadStrategy == BufferedLoad && cfg.MaxCacheSize <= 0 {
		return errors.Wrap(ErrInvalidConfiguration, "MaxCacheSize must be set explicitly for BufferedLoad")
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

func (cfg *Config) ensureInMemoryConfiguration(ee executionEngine) error {
	if cfg.PersistenceStrategy != "" && cfg.PersistenceStrategy != InMemory {
		return errors.Wrapf(
			ErrInvalidConfiguration,
			"in memory engine can not have persistence strategy %s",
			cfg.PersistenceStrategy,
		)
	}

	if cfg.ValueLoadStrategy != "" && cfg.ValueLoadStrategy != EagerLoad {
		return errors.Wrap(
			ErrInvalidConfiguration,
			"in memory engine should not have value load strategy",
		)
	}

	if cfg.AutoVacuumOnlyOnCloseOrFlush || cfg.AutoVacuumIntervals != 0 || cfg.AutoVacuumMinSize != 0 {
		return errors.Wrap(
			ErrInvalidConfiguration,
			"in memory engine does not use auto vacuum, remove all related parameters from config",
		)
	}

	cfg.PersistenceStrategy = InMemory
	cfg.ValueLoadStrategy = EagerLoad
	cfg.DisableAutoVacuum = true

	ee.SetCfg(cfg)

	return nil
}