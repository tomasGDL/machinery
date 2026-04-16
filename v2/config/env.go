package config

import (
	"github.com/RichardKnop/machinery/v2/log"
	"github.com/kelseyhightower/envconfig"
)

// NewFromEnvironment creates a config object from environment variables
func NewFromEnvironment() (*Config, error) {
	cnf, err := fromEnvironment()
	if err != nil {
		return nil, err
	}

	log.INFO.Print("Successfully loaded config from the environment")

	return cnf, nil
}

func fromEnvironment() (*Config, error) {
	cnf := new(Config)
	*cnf = *defaultCnf

	if err := envconfig.Process("", cnf); err != nil {
		return nil, err
	}

	return cnf, nil
}
