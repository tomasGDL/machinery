package batchqueue

// label definition.
type Label struct {
	Name  string
	Value string
}

type Identifier interface {
	// returns unique identifier for entry.
	EntryID() string
	// returns binded labels
	Labels() []Label
	// Duplicate returns copy instance
	Duplicate() Identifier
}

type UnimplementedIder struct{}

func (i UnimplementedIder) EntryID() string { return "" }

func (i UnimplementedIder) Labels() []Label { return []Label{} }

func (i UnimplementedIder) Duplicate() Identifier { return i }
