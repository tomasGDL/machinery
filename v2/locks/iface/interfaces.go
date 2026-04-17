package iface

// Lock defines the interface for distributed lock implementations
type Lock interface {
	// LockWithRetries acquires the lock with retry mechanism
	// key: the name of the lock
	// value: the nanosecond timestamp when the lock should be released automatically
	LockWithRetries(key string, value int64) error

	// Lock acquires the lock once without retry
	// key: the name of the lock
	// value: the nanosecond timestamp when the lock should be released automatically
	Lock(key string, value int64) error
}
