package relatalg

// Rename renames a column in a relation.
// The replaced column name must be part of the relation's
// schema, and the new column name must not already be a
// part of the relation's schema.
func Rename(r Relation, oldCol, newCol Column) Relation {
	if _, ok := r.Schema()[oldCol]; !ok {
		panic("replaced column is not in schema: " + oldCol.String())
	}
	if _, ok := r.Schema()[newCol]; ok {
		panic("new column already in schema: " + newCol.String())
	}
	resChan := make(chan Row, 1)
	go func() {
		defer close(resChan)
		for row := range r.Entries() {
			resChan <- renameColumn(row, oldCol, newCol)
		}
	}()
	newSchema := map[Column]Type{}
	for k, v := range r.Schema() {
		if k != oldCol {
			newSchema[k] = v
		} else {
			newSchema[newCol] = v
		}
	}
	return &ConcreteRelation{
		S: newSchema,
		E: resChan,
	}
}

func renameColumn(r Row, oldCol, newCol Column) Row {
	res := Row{}
	for k, v := range r {
		if k != oldCol {
			res[k] = v
		} else {
			res[newCol] = v
		}
	}
	return res
}
