package relatalg

import "testing"

func TestDistinct(t *testing.T) {
	r1Chan := make(chan Row, 10)
	r2Chan := make(chan Row, 4)
	r1Chan <- Row{ParseColumn("R1.id"): 0, ParseColumn("R1.name"): "Bob"}
	r1Chan <- Row{ParseColumn("R1.id"): 1, ParseColumn("R1.name"): "Joe"}
	r1Chan <- Row{ParseColumn("R1.id"): 2, ParseColumn("R1.name"): "Hal"}
	r1Chan <- Row{ParseColumn("R1.id"): 0, ParseColumn("R1.name"): "Bob"}
	r1Chan <- Row{ParseColumn("R1.id"): 1, ParseColumn("R1.name"): "Joe"}
	r1Chan <- Row{ParseColumn("R1.id"): 2, ParseColumn("R1.name"): "Hal"}
	r1Chan <- Row{ParseColumn("R1.id"): 3, ParseColumn("R1.name"): "Bob"}
	r1Chan <- Row{ParseColumn("R1.id"): 1, ParseColumn("R1.name"): "Joe"}
	r1Chan <- Row{ParseColumn("R1.id"): 2, ParseColumn("R1.name"): "Hal"}
	r1Chan <- Row{ParseColumn("R1.id"): 3, ParseColumn("R1.name"): "Bob"}
	r2Chan <- Row{ParseColumn("R1.id"): 0, ParseColumn("R1.name"): "Bob"}
	r2Chan <- Row{ParseColumn("R1.id"): 1, ParseColumn("R1.name"): "Joe"}
	r2Chan <- Row{ParseColumn("R1.id"): 2, ParseColumn("R1.name"): "Hal"}
	r2Chan <- Row{ParseColumn("R1.id"): 3, ParseColumn("R1.name"): "Bob"}
	close(r1Chan)
	close(r2Chan)

	r1 := &ConcreteRelation{
		S: map[Column]Type{ParseColumn("R1.id"): Integer, ParseColumn("R1.name"): String},
		E: r1Chan,
	}
	expected := &ConcreteRelation{
		S: map[Column]Type{ParseColumn("R1.id"): Integer, ParseColumn("R1.name"): String},
		E: r2Chan,
	}
	actual := Distinct(r1)
	if !Equal(actual, expected) {
		t.Error("distinct returned invalid result")
	}
}

func TestSubtract(t *testing.T) {
	r1Chan := make(chan Row, 6)
	r2Chan := make(chan Row, 5)
	r3Chan := make(chan Row, 2)

	r1Chan <- Row{ParseColumn("id"): 0}
	r1Chan <- Row{ParseColumn("id"): 1}
	r1Chan <- Row{ParseColumn("id"): 2}
	r1Chan <- Row{ParseColumn("id"): 1}
	r1Chan <- Row{ParseColumn("id"): 2}
	r1Chan <- Row{ParseColumn("id"): 3}

	r2Chan <- Row{ParseColumn("id"): 0}
	r2Chan <- Row{ParseColumn("id"): 1}
	r2Chan <- Row{ParseColumn("id"): 2}
	r2Chan <- Row{ParseColumn("id"): 2}
	r2Chan <- Row{ParseColumn("id"): 2}

	r3Chan <- Row{ParseColumn("id"): 1}
	r3Chan <- Row{ParseColumn("id"): 3}

	close(r1Chan)
	close(r2Chan)
	close(r3Chan)

	r1 := &ConcreteRelation{
		S: map[Column]Type{ParseColumn("id"): Integer},
		E: r1Chan,
	}
	r2 := &ConcreteRelation{
		S: map[Column]Type{ParseColumn("id"): Integer},
		E: r2Chan,
	}
	actual := Subtract(r1, r2)
	expected := &ConcreteRelation{
		S: map[Column]Type{ParseColumn("id"): Integer},
		E: r3Chan,
	}
	if !Equal(actual, expected) {
		t.Error("invalid result")
	}
}
