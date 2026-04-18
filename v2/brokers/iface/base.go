package iface

import (
	"errors"
	"sync"

	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/log"
	"github.com/RichardKnop/machinery/v2/retry"
	"github.com/RichardKnop/machinery/v2/tasks"
)

type registeredTaskNames struct {
	sync.RWMutex
	items []string
}

// BaseBroker represents a base broker structure
type BaseBroker struct {
	cnf                 *config.Config
	registeredTaskNames registeredTaskNames
	retry               bool
	retryFunc           func(chan int)
	retryStopChan       chan int
	stopChan            chan int
}

// NewBaseBroker creates new BaseBroker instance
func NewBaseBroker(cnf *config.Config) BaseBroker {
	return BaseBroker{
		cnf:           cnf,
		retry:         true,
		stopChan:      make(chan int),
		retryStopChan: make(chan int),
	}
}

// GetConfig returns config
func (b *BaseBroker) GetConfig() *config.Config {
	return b.cnf
}

// GetRetry ...
func (b *BaseBroker) GetRetry() bool {
	return b.retry
}

// GetRetryFunc ...
func (b *BaseBroker) GetRetryFunc() func(chan int) {
	return b.retryFunc
}

// GetRetryStopChan ...
func (b *BaseBroker) GetRetryStopChan() chan int {
	return b.retryStopChan
}

// GetStopChan ...
func (b *BaseBroker) GetStopChan() chan int {
	return b.stopChan
}

// Publish places a new message on the default queue
func (b *BaseBroker) Publish(signature *tasks.Signature) error {
	return errors.New("Not implemented")
}

// SetRegisteredTaskNames sets registered task names
func (b *BaseBroker) SetRegisteredTaskNames(names []string) {
	b.registeredTaskNames.Lock()
	defer b.registeredTaskNames.Unlock()
	b.registeredTaskNames.items = names
}

// IsTaskRegistered returns true if the task is registered with this broker
func (b *BaseBroker) IsTaskRegistered(name string) bool {
	b.registeredTaskNames.RLock()
	defer b.registeredTaskNames.RUnlock()
	for _, registeredTaskName := range b.registeredTaskNames.items {
		if registeredTaskName == name {
			return true
		}
	}
	return false
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (b *BaseBroker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	return nil, errors.New("Not implemented")
}

// GetDelayedTasks returns a slice of task.Signatures that are scheduled, but not yet in the queue
func (b *BaseBroker) GetDelayedTasks() ([]*tasks.Signature, error) {
	return nil, errors.New("Not implemented")
}

// StartConsuming is a common part of StartConsuming method
func (b *BaseBroker) StartConsuming(consumerTag string, concurrency int, taskProcessor TaskProcessor) {
	if b.retryFunc == nil {
		b.retryFunc = retry.Closure()
	}

}

// StopConsuming is a common part of StopConsuming
func (b *BaseBroker) StopConsuming() {
	// Do not retry from now on
	b.retry = false
	// Stop the retry closure earlier
	select {
	case b.retryStopChan <- 1:
		log.GetLogger().Warnf("Stopping retry closure.")
	default:
	}
	// Notifying the stop channel stops consuming of messages
	close(b.stopChan)
	log.GetLogger().Warnf("Stop channel")
}

// GetRegisteredTaskNames returns registered tasks names
func (b *BaseBroker) GetRegisteredTaskNames() []string {
	b.registeredTaskNames.RLock()
	defer b.registeredTaskNames.RUnlock()
	items := b.registeredTaskNames.items
	return items
}

// AdjustRoutingKey makes sure the routing key is correct.
// If the routing key is an empty string:
// a) set it to binding key for direct exchange type
// b) set it to default queue name
func (b *BaseBroker) AdjustRoutingKey(s *tasks.Signature) {
	if s.RoutingKey != "" {
		return
	}

	s.RoutingKey = b.GetConfig().DefaultQueue
}
