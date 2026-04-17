package iface

import (
	"context"

	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/tasks"
)

// Broker - a common interface for all brokers
type Broker interface {
	// GetConfig returns the broker configuration
	GetConfig() *config.Config

	// SetRegisteredTaskNames sets the list of registered task names
	SetRegisteredTaskNames(names []string)

	// IsTaskRegistered returns true if the task name is registered
	IsTaskRegistered(name string) bool

	// StartConsuming enters a loop and waits for incoming messages
	// Returns retry flag and error if any
	StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)

	// StopConsuming quits the message consumption loop
	StopConsuming()

	// Publish places a new message on the queue
	Publish(ctx context.Context, task *tasks.Signature) error

	// GetPendingTasks returns a slice of task signatures waiting in the queue
	GetPendingTasks(queue string) ([]*tasks.Signature, error)

	// GetDelayedTasks returns a slice of task signatures scheduled but not yet in the queue
	GetDelayedTasks() ([]*tasks.Signature, error)

	// AdjustRoutingKey adjusts the routing key for the task signature
	AdjustRoutingKey(s *tasks.Signature)
}

// TaskProcessor - can process a delivered task
// This will probably always be a worker instance
type TaskProcessor interface {
	// Process processes a task signature
	Process(signature *tasks.Signature) error

	// CustomQueue returns the custom queue name, empty string means default queue
	CustomQueue() string

	// PreConsumeHandler is called before consuming a task
	// Returns true to continue, false to skip
	PreConsumeHandler() bool
}
