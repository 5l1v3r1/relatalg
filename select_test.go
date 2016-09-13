package relatalg

import "testing"

func TestSelect(t *testing.T) {
	r1Chan := make(chan Row, 5)
	r2Chan := make(chan Row, 3)
	r1Chan <- Row{ParseColumn("id"): 0, ParseColumn("age"): 1}
	r1Chan <- Row{ParseColumn("id"): 1, ParseColumn("age"): 3}
	r1Chan <- Row{ParseColumn("id"): 1, ParseColumn("age"): 5}
	r1Chan <- Row{ParseColumn("id"): 1, ParseColumn("age"): 7}
	r1Chan <- Row{ParseColumn("id"): 1, ParseColumn("age"): 9}
	r2Chan <- Row{ParseColumn("id"): 1, ParseColumn("age"): 5}
	r2Chan <- Row{ParseColumn("id"): 1, ParseColumn("age"): 7}
	r2Chan <- Row{ParseColumn("id"): 1, ParseColumn("age"): 9}
	close(r1Chan)
	close(r2Chan)
	r1 := &ConcreteRelation{
		S: map[Column]Type{ParseColumn("id"): Integer, ParseColumn("age"): Integer},
		E: r1Chan,
	}
	expected := &ConcreteRelation{
		S: map[Column]Type{ParseColumn("id"): Integer, ParseColumn("age"): Integer},
		E: r2Chan,
	}
	actual := Select(r1, &Not{P: &LtVal{Column: ParseColumn("age"), Value: 5}})

	if !Equal(expected, actual) {
		t.Error("invalid selection result")
	}
}
