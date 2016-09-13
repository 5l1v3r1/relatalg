package relatalg

// Distinct removes duplicates from a relation.
func Distinct(r Relation) Relation {
	resChan := make(chan Row, 1)
	go func() {
		defer close(resChan)
		var seen []Row
		for x := range r.Entries() {
			hasSeen := false
			for _, y := range seen {
				if rowsEqual(x, y) {
					hasSeen = true
					break
				}
			}
			if !hasSeen {
				seen = append(seen, x)
				resChan <- x
			}
		}
	}()
	return &ConcreteRelation{
		E: resChan,
		S: r.Schema(),
	}
}
