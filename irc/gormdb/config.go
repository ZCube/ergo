// Copyright (c) 2020 Shivaram Lingamneni
// released under the MIT license

package gormdb

import (
	"time"
)

type Config struct {
	// these are intended to be written directly into the config file:
	Enabled         bool
	Driver          string `yaml:"driver"`
	DSN             string `yaml:"dsn"`
	HistoryDatabase string `yaml:"history-database"`
	Debug           bool   `yaml:"debug"`
	Timeout         time.Duration
	MaxConns        int           `yaml:"max-conns"`
	ConnMaxLifetime time.Duration `yaml:"conn-max-lifetime"`

	// XXX these are copied from elsewhere in the config:
	ExpireTime           time.Duration
	TrackAccountMessages bool
}
