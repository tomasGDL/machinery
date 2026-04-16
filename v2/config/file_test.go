package config_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v2/config"
	"github.com/stretchr/testify/assert"
)

var configYAMLData = `---
broker: broker
default_queue: default_queue
result_backend: result_backend
results_expire_in: 123456
redis:
  max_idle: 12
  max_active: 123
  max_idle_timeout: 456
  wait: false
  read_timeout: 17
  write_timeout: 19
  connect_timeout: 21
  normal_tasks_poll_period: 1001
  delayed_tasks_poll_period: 23
  delayed_tasks_key: delayed_tasks_key
  master_name: master_name
no_unix_signals: true
`

func TestReadFromFile(t *testing.T) {
	t.Parallel()

	data, err := config.ReadFromFile("testconfig.yml")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, configYAMLData, string(data))
}

func TestNewFromYaml(t *testing.T) {
	t.Parallel()

	cnf, err := config.NewFromYaml("testconfig.yml", false)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "broker", cnf.Broker)
	assert.Equal(t, "default_queue", cnf.DefaultQueue)
	assert.Equal(t, "result_backend", cnf.ResultBackend)
	assert.Equal(t, 123456, cnf.ResultsExpireIn)

	assert.Equal(t, 12, cnf.Redis.MaxIdle)
	assert.Equal(t, 123, cnf.Redis.MaxActive)
	assert.Equal(t, 456, cnf.Redis.IdleTimeout)
	assert.Equal(t, false, cnf.Redis.Wait)
	assert.Equal(t, 17, cnf.Redis.ReadTimeout)
	assert.Equal(t, 19, cnf.Redis.WriteTimeout)
	assert.Equal(t, 21, cnf.Redis.ConnectTimeout)
	assert.Equal(t, 1001, cnf.Redis.NormalTasksPollPeriod)
	assert.Equal(t, 23, cnf.Redis.DelayedTasksPollPeriod)
	assert.Equal(t, "delayed_tasks_key", cnf.Redis.DelayedTasksKey)
	assert.Equal(t, "master_name", cnf.Redis.MasterName)

	assert.Equal(t, true, cnf.NoUnixSignals)
}
