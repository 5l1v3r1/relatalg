package relatalg

import "fmt"

// A Predicate filters certain rows by checking if they
// satisfy some set of criteria.
type Predicate interface {
	Satisfies(r Row) bool
}

// EqVal is a Predicate that checks if an entry in a row
// is equal to a specific value.
type EqVal struct {
	Column Column
	Value  interface{}
}

// Satisfies returns true if the entry in the row matches
// e.Value.
func (e *EqVal) Satisfies(r Row) bool {
	return entriesEqual(r[e.Column], e.Value)
}

// Eq is a Predicate that checks if an entry in a row is
// equal to another entry in the same row.
type Eq struct {
	Column1 Column
	Column2 Column
}

// Satisfies returns true if the two entries in the row
// are equal.
func (e *Eq) Satisfies(r Row) bool {
	return entriesEqual(r[e.Column1], r[e.Column2])
}

// LtVal is a Predicate that checks if an entry in a row
// is less than a given value.
type LtVal struct {
	Column Column
	Value  interface{}
}

// Satisfies returns true if the entry in the row is less
// than the given value.
func (l *LtVal) Satisfies(r Row) bool {
	return entriesLessThan(r[l.Column], l.Value)
}

// Lt is a Predicate that checks if an entry in a row is
// less than another entry in that row.
type Lt struct {
	Column1 Column
	Column2 Column
}

// Satisfies returns true if the Column1 value of the
// entry is less than the Column2 value.
func (l *Lt) Satisfies(r Row) bool {
	return entriesLessThan(r[l.Column1], r[l.Column2])
}

// Not negates a predicate.
type Not struct {
	P Predicate
}

// Satisfies returns the negated form of n.P.
func (n *Not) Satisfies(r Row) bool {
	return !n.P.Satisfies(r)
}

// And combines predicates into a predicate that is only
// satisfied if all sub-predicates are.
type And []Predicate

// Satisfies returns true if all the sub-predicates are
// satisfied.
func (a And) Satisfies(r Row) bool {
	for _, x := range a {
		if !x.Satisfies(r) {
			return false
		}
	}
	return true
}

// Or combines predicates into a predicate that is
// satisfied if any sub-predicate is.
type Or []Predicate

// Satisfies returns true if any sub-predicate does.
func (o Or) Satisfies(r Row) bool {
	for _, x := range o {
		if x.Satisfies(r) {
			return true
		}
	}
	return false
}

// Select restricts a Relation to those entries which are
// satisfied by the predicate.
func Select(r Relation, p Predicate) Relation {
	resChan := make(chan Row, 1)
	go func() {
		defer close(resChan)
		for x := range r.Entries() {
			if p.Satisfies(x) {
				resChan <- x
			}
		}
	}()
	return &ConcreteRelation{
		S: r.Schema(),
		E: resChan,
	}
}

func entriesLessThan(v1, v2 interface{}) bool {
	switch v1 := v1.(type) {
	case int:
		return v1 < v2.(int)
	case float64:
		return v1 < v2.(float64)
	default:
		panic(fmt.Sprintf("invalid type for less-than comparison: %T", v1))
	}
}
