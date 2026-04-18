package iface

import (
	"github.com/RichardKnop/machinery/v2/config"
)

// BaseBackend represents a base backend structure
type BaseBackend struct {
	cnf *config.Config
}

// NewBaseBackend creates new Backend instance
func NewBaseBackend(cnf *config.Config) BaseBackend {
	return BaseBackend{cnf: cnf}
}

// GetConfig returns config
func (b *BaseBackend) GetConfig() *config.Config {
	return b.cnf
}
