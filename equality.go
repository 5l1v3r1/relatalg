package relatalg

import "bytes"

// Equal compares two relations for equality.
// Relations are considered equal if they contain
// the same rows the same number of times.
// Relations cannot be equal if they have different
// schemas.
func Equal(r1, r2 Relation) bool {
	if !schemaContains(r1, r2) || !schemaContains(r2, r1) {
		return false
	}
	var unmatchedValues multiset
	for row := range r1.Entries() {
		unmatchedValues.Add(row)
	}
	for row := range r2.Entries() {
		if !unmatchedValues.Contains(row) {
			return false
		}
		unmatchedValues.Remove(row)
	}
	return unmatchedValues.Len() == 0
}

func schemaContains(subSchema, superSchema Relation) bool {
	for column, t := range subSchema.Schema() {
		if t1, ok := superSchema.Schema()[column]; !ok {
			return false
		} else if t1 != t {
			return false
		}
	}
	return true
}

func rowsEqual(r1, r2 Row) bool {
	for key, val := range r1 {
		if !entriesEqual(val, r2[key]) {
			return false
		}
	}
	return true
}

func entriesEqual(val1, val2 interface{}) bool {
	switch val1 := val1.(type) {
	case []byte:
		return bytes.Equal(val1, val2.([]byte))
	}
	return val1 == val2
}
