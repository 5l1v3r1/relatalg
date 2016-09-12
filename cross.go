package relatalg

// Cartesian crosses two columns, combining each of the
// tuples from x with each of the tuples from y.
// The two relations must not have any identical columns.
func Cartesian(x, y Relation) Relation {
    for column := range x.Schema() {
        if _, ok := y.Schema()[column]; ok {
            panic("schema overlap: " + column.String())
        }
    }
    jointSchema := map[Column]Type{}
    for col, t := range x.Schema() {
        jointSchema[col] = t
    }
    for col, t := range y.Schema() {
        jointSchema[col] = t
    }
    resChan := make(chan Row, 1)
    go func() {
        defer close(resChan)
        var xRows []Row
        for row := range x.Entries() {
            xRows = append(xRows, val)
        }
        for yRow := range y.Entries() {
            for _, xRow := range xRows {
                resChan <- crossRows(xRow, yRow)
            }
        }
    }
}

func crossRows(r1, r2 Row) Row {
    res := Row{}
    for _, r := range []Row{r1, r2} {
        for key, val := range r {
            res[key] = val
        }
    }
}
