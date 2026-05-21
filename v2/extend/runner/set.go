package runner

// stringSet 是一个简单的字符串集合，用于替代 pam/backend/pkg/set
type stringSet struct {
	items map[string]struct{}
}

func newStringSet() *stringSet {
	return &stringSet{
		items: make(map[string]struct{}),
	}
}

func (s *stringSet) Add(val string) {
	s.items[val] = struct{}{}
}

func (s *stringSet) Has(val string) bool {
	_, ok := s.items[val]
	return ok
}

func (s *stringSet) Slice() []string {
	result := make([]string, 0, len(s.items))
	for k := range s.items {
		result = append(result, k)
	}
	return result
}

// intSet 是一个简单的整数集合，用于替代 pam/backend/pkg/set
type intSet struct {
	items map[int]struct{}
}

func newIntSet(vals ...int) *intSet {
	s := &intSet{
		items: make(map[int]struct{}),
	}
	for _, v := range vals {
		s.items[v] = struct{}{}
	}
	return s
}

func (s *intSet) Add(val int) {
	s.items[val] = struct{}{}
}

func (s *intSet) Has(val int) bool {
	_, ok := s.items[val]
	return ok
}

func (s *intSet) Slice() []int {
	result := make([]int, 0, len(s.items))
	for k := range s.items {
		result = append(result, k)
	}
	return result
}
