package iface

import (
	"github.com/RichardKnop/machinery/v2/tasks"
)

// Backend - a common interface for all result backends
type Backend interface {
	// Group related functions

	// InitGroup creates and saves a group meta data object for tracking group tasks
	InitGroup(groupUUID string, taskUUIDs []string) error

	// GroupCompleted returns true if all tasks in a group have finished (success or failure)
	GroupCompleted(groupUUID string, groupTaskCount int) (bool, error)

	// GroupTaskStates returns states of all tasks in the group
	GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error)

	// TriggerChord flags chord as triggered to ensure it is never triggered multiple times
	// Returns true if the worker should trigger chord, false if already triggered
	TriggerChord(groupUUID string) (bool, error)

	// Setting / getting task state

	// SetStatePending updates task state to PENDING
	SetStatePending(signature *tasks.Signature) error

	// SetStateReceived updates task state to RECEIVED
	SetStateReceived(signature *tasks.Signature) error

	// SetStateStarted updates task state to STARTED
	SetStateStarted(signature *tasks.Signature) error

	// SetStateRetry updates task state to RETRY
	SetStateRetry(signature *tasks.Signature) error

	// SetStateSuccess updates task state to SUCCESS with task results
	SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error

	// SetStateFailure updates task state to FAILURE with error message
	SetStateFailure(signature *tasks.Signature, err string) error

	// GetState returns the latest task state by task UUID
	GetState(taskUUID string) (*tasks.TaskState, error)

	// Purging stored tasks states and group meta data

	// PurgeState deletes stored task state
	PurgeState(taskUUID string) error

	// PurgeGroupMeta deletes stored group meta data
	PurgeGroupMeta(groupUUID string) error
}
