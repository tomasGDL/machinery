package iface_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v2"
	"github.com/RichardKnop/machinery/v2/brokers/iface"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/stretchr/testify/assert"
)

func TestIsTaskRegistered(t *testing.T) {
	t.Parallel()

	broker := iface.NewBaseBroker(new(config.Config))
	broker.SetRegisteredTaskNames([]string{"foo", "bar"})

	assert.True(t, broker.IsTaskRegistered("foo"))
	assert.False(t, broker.IsTaskRegistered("bogus"))
}

func TestAdjustRoutingKey(t *testing.T) {
	t.Parallel()

	var (
		s      *tasks.Signature
		broker iface.BaseBroker
	)

	t.Run("with routing key", func(t *testing.T) {
		s = &tasks.Signature{RoutingKey: "routing_key"}
		broker = iface.NewBaseBroker(&config.Config{
			DefaultQueue: "queue",
		})
		broker.AdjustRoutingKey(s)
		assert.Equal(t, "routing_key", s.RoutingKey)
	})

	t.Run("without routing key", func(t *testing.T) {
		s = new(tasks.Signature)
		broker = iface.NewBaseBroker(&config.Config{
			DefaultQueue: "queue",
		})
		broker.AdjustRoutingKey(s)
		assert.Equal(t, "queue", s.RoutingKey)
	})
}

func TestGetRegisteredTaskNames(t *testing.T) {
	t.Parallel()

	broker := iface.NewBaseBroker(new(config.Config))
	fooTasks := []string{"foo", "bar", "baz"}
	broker.SetRegisteredTaskNames(fooTasks)
	assert.Equal(t, fooTasks, broker.GetRegisteredTaskNames())
}

func TestStopConsuming(t *testing.T) {
	t.Parallel()

	t.Run("stop consuming", func(t *testing.T) {
		broker := iface.NewBaseBroker(&config.Config{
			DefaultQueue: "queue",
		})
		broker.StartConsuming("", 1, &machinery.Worker{})
		broker.StopConsuming()
		select {
		case <-broker.GetStopChan():
		default:
			assert.Fail(t, "still blocking")
		}
	})
}
