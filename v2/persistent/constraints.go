package persistent

import (
	"github.com/RichardKnop/machinery/v2/persistent/iface"
)

// SubjectConcurrencyConstraint 主体并发约束
type SubjectConcurrencyConstraint struct {
	maxConcurrent int
}

func NewSubjectConcurrencyConstraint(maxConcurrent int) *SubjectConcurrencyConstraint {
	return &SubjectConcurrencyConstraint{maxConcurrent: maxConcurrent}
}

func (c *SubjectConcurrencyConstraint) Name() string {
	return "subject_concurrency"
}

func (c *SubjectConcurrencyConstraint) Check(entry *iface.PersistentEntry, scheduled []*iface.ScheduledSignature) bool {
	if entry.SubjectID == "" || c.maxConcurrent <= 0 {
		return true
	}

	count := 0
	for _, s := range scheduled {
		if s.SubjectID == entry.SubjectID && s.SubjectType == entry.SubjectType {
			count++
		}
	}
	return count < c.maxConcurrent
}

// AccountConcurrencyConstraint 账号并发约束
type AccountConcurrencyConstraint struct {
	maxConcurrent int
}

func NewAccountConcurrencyConstraint(maxConcurrent int) *AccountConcurrencyConstraint {
	return &AccountConcurrencyConstraint{maxConcurrent: maxConcurrent}
}

func (c *AccountConcurrencyConstraint) Name() string {
	return "account_concurrency"
}

func (c *AccountConcurrencyConstraint) Check(entry *iface.PersistentEntry, scheduled []*iface.ScheduledSignature) bool {
	if entry.SubjectID == "" || c.maxConcurrent <= 0 {
		return true
	}

	count := 0
	for _, s := range scheduled {
		if s.SubjectID == entry.SubjectID {
			count++
		}
	}
	return count < c.maxConcurrent
}

// GroupConcurrencyConstraint 分组并发约束
type GroupConcurrencyConstraint struct {
	maxConcurrent int
}

func NewGroupConcurrencyConstraint(maxConcurrent int) *GroupConcurrencyConstraint {
	return &GroupConcurrencyConstraint{maxConcurrent: maxConcurrent}
}

func (c *GroupConcurrencyConstraint) Name() string {
	return "group_concurrency"
}

func (c *GroupConcurrencyConstraint) Check(entry *iface.PersistentEntry, scheduled []*iface.ScheduledSignature) bool {
	if entry.GroupID == "" || c.maxConcurrent <= 0 {
		return true
	}

	count := 0
	for _, s := range scheduled {
		if s.GroupID == entry.GroupID {
			count++
		}
	}
	return count < c.maxConcurrent
}
