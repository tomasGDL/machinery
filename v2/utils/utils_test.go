package utils

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetLockName(t *testing.T) {
	t.Parallel()

	lockName := GetLockName("test", "*/3 * * *")
	// Check that lock name contains expected parts
	assert.True(t, strings.HasPrefix(lockName, LockKeyPrefix))
	assert.True(t, strings.Contains(lockName, "test"))
	assert.True(t, strings.Contains(lockName, "*/3 * * *"))
}
