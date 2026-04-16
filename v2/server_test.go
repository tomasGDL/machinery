package machinery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/RichardKnop/machinery/v2"
	"github.com/RichardKnop/machinery/v2/config"

	redisbackend "github.com/RichardKnop/machinery/v2/backends/redis"
	redisbroker "github.com/RichardKnop/machinery/v2/brokers/redis"
	redislock "github.com/RichardKnop/machinery/v2/locks/redis"
)

func TestRegisterTasks(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	err := server.RegisterTasks(map[string]interface{}{
		"test_task": func() error { return nil },
	})
	assert.NoError(t, err)

	_, err = server.GetRegisteredTask("test_task")
	assert.NoError(t, err, "test_task is not registered but it should be")
}

func TestRegisterTask(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	err := server.RegisterTask("test_task", func() error { return nil })
	assert.NoError(t, err)

	_, err = server.GetRegisteredTask("test_task")
	assert.NoError(t, err, "test_task is not registered but it should be")
}

func TestGetRegisteredTask(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	_, err := server.GetRegisteredTask("test_task")
	assert.Error(t, err, "test_task is registered but it should not be")
}

func TestGetRegisteredTaskNames(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)

	taskName := "test_task"
	err := server.RegisterTask(taskName, func() error { return nil })
	assert.NoError(t, err)

	taskNames := server.GetRegisteredTaskNames()
	assert.Equal(t, 1, len(taskNames))
	assert.Equal(t, taskName, taskNames[0])
}

func TestNewWorker(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)

	server.NewWorker("test_worker", 1)
	assert.NoError(t, nil)
}

func TestNewCustomQueueWorker(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)

	server.NewCustomQueueWorker("test_customqueueworker", 1, "test_queue")
	assert.NoError(t, nil)
}

func getTestServer(t *testing.T) *machinery.Server {
	cnf := &config.Config{
		Broker:        "redis://localhost:6379",
		ResultBackend: "redis://localhost:6379",
		Lock:          "redis://localhost:6379",
		Redis: &config.RedisConfig{
			MaxIdle:     3,
			IdleTimeout: 240,
			ReadTimeout: 15,
		},
	}

	broker := redisbroker.New(cnf, client)
	backend := redisbackend.New(cnf, client)
	lock := redislock.New(client, 3)
	return machinery.NewServer(cnf, broker, backend, lock)
}
