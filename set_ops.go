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
