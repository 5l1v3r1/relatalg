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
