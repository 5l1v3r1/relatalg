// Package relatalg implements various abstractions for
// relational algebra.
package relatalg

import "strings"

// A Type represents the type of data stored in a column.
type Type int

// These are the types that columns may have.
//
// Integer corresponds to int.
// Real corresponds to float64.
// String corresponds to string.
// Blob corresponds to []byte.
// Bool corresponds to a bool.
const (
	Integer Type = iota
	Real
	String
	Blob
	Bool
)

// Column defines a column's name in a relation.
// Columns may have optional namespaces, which can be
// used to distinguish identically-named columns while
// keeping them equivalent for operations like joins.
type Column struct {
	Namespace string
	Name      string
}

// ParseColumn parses a column's name and creates the
// resulting Column.
// Column names may use a "." to separate the namespace
// from the key name, e.g. "MyNamespace.MyKey".
func ParseColumn(s string) Column {
	dotIdx := strings.Index(s, ".")
	if dotIdx < 0 {
		return Column{Name: s}
	} else {
		return Column{Namespace: s[:dotIdx], Name: s[dotIdx+1:]}
	}
}

// String returns the human-readable representation of the
// column's name.
// If there is a namespace, it is separated by a ".".
// If there is no namespace, the plain name is returned.
func (c Column) String() string {
	if c.Namespace == "" {
		return c.Name
	} else {
		return c.Namespace + "." + c.Name
	}
}

// A Row represents an entry in a relation.
type Row map[Column]interface{}

// A Relation is a multiset of rows for a given schema.
type Relation interface {
	Schema() map[Column]Type
	Entries() <-chan Row
}

// A ConcreteRelation stores a pre-determined schema and
// channel of entries and returns said values.
type ConcreteRelation struct {
	S map[Column]Type
	E <-chan Row
}

// Schema returns c.S.
func (c *ConcreteRelation) Schema() map[Column]Type {
	return c.S
}

// Entries returns c.E.
func (c *ConcreteRelation) Entries() <-chan Row {
	return c.E
}
