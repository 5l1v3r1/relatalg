package relatalg

// Distinct removes duplicates from a relation.
func Distinct(r Relation) Relation {
	resChan := make(chan Row, 1)
	go func() {
		defer close(resChan)
		var seen multiset
		for x := range r.Entries() {
			if !seen.Contains(x) {
				seen.Add(x)
				resChan <- x
			}
		}
	}()
	return &ConcreteRelation{
		E: resChan,
		S: r.Schema(),
	}
}

// Subtract removes the elements of b from a.
// It follows the subtraction rules for multisets.
// For example, if b contains an element once and a
// contains it twice, the result will contain it once.
//
// The schemas of a and b must match.
func Subtract(a, b Relation) Relation {
	if !schemaContains(a, b) || !schemaContains(b, a) {
		panic("schemas must match")
	}
	var bSet multiset
	for r := range b.Entries() {
		bSet.Add(r)
	}
	resChan := make(chan Row, 1)
	go func() {
		defer close(resChan)
		for r := range a.Entries() {
			if bSet.Contains(r) {
				bSet.Remove(r)
			} else {
				resChan <- r
			}
		}
	}()
	return &ConcreteRelation{
		E: resChan,
		S: a.Schema(),
	}
}

// Union produces a Relation with all the elements from
// a and b.
func Union(a, b Relation) Relation {
	if !schemaContains(a, b) || !schemaContains(b, a) {
		panic("schemas must match")
	}
	resChan := make(chan Row, 1)
	go func() {
		defer close(resChan)
		for r := range a.Entries() {
			resChan <- r
		}
		for r := range b.Entries() {
			resChan <- r
		}
	}()
	return &ConcreteRelation{
		E: resChan,
		S: a.Schema(),
	}
}

// Intersection produces a Relation with the elements that
// are in both a and b.
func Intersection(a, b Relation) Relation {
	if !schemaContains(a, b) || !schemaContains(b, a) {
		panic("schemas must match")
	}
	resChan := make(chan Row, 1)
	go func() {
		defer close(resChan)
		var available multiset
		for r := range a.Entries() {
			available.Add(r)
		}
		for r := range b.Entries() {
			if available.Contains(r) {
				available.Remove(r)
				resChan <- r
			}
		}
	}()
	return &ConcreteRelation{
		E: resChan,
		S: a.Schema(),
	}
}

type multiset struct {
	entries []Row
}

func (m *multiset) Contains(r Row) bool {
	for _, x := range m.entries {
		if rowsEqual(x, r) {
			return true
		}
	}
	return false
}

func (m *multiset) Remove(r Row) {
	for i, x := range m.entries {
		if rowsEqual(x, r) {
			m.entries[i] = m.entries[len(m.entries)-1]
			m.entries = m.entries[:len(m.entries)-1]
			break
		}
	}
}

func (m *multiset) Add(r Row) {
	m.entries = append(m.entries, r)
}

func (m *multiset) Len() int {
	return len(m.entries)
}
