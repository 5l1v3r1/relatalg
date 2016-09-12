package relatalg

import "testing"

func TestRename(t *testing.T) {
	r1Chan := make(chan Row, 3)
	r2Chan := make(chan Row, 3)
	r1Chan <- Row{ParseColumn("R1.id"): 0, ParseColumn("R1.name"): "Bob"}
	r1Chan <- Row{ParseColumn("R1.id"): 1, ParseColumn("R1.name"): "Joe"}
	r1Chan <- Row{ParseColumn("R1.id"): 2, ParseColumn("R1.name"): "Hal"}
	r2Chan <- Row{ParseColumn("R2.id"): 0, ParseColumn("name"): "Bob"}
	r2Chan <- Row{ParseColumn("R2.id"): 1, ParseColumn("name"): "Joe"}
	r2Chan <- Row{ParseColumn("R2.id"): 2, ParseColumn("name"): "Hal"}
	close(r1Chan)
	close(r2Chan)
	r1 := &ConcreteRelation{
		S: map[Column]Type{ParseColumn("R1.id"): Integer, ParseColumn("R1.name"): String},
		E: r1Chan,
	}
	expected := &ConcreteRelation{
		S: map[Column]Type{ParseColumn("R2.id"): Integer, ParseColumn("name"): String},
		E: r2Chan,
	}
	actual := Rename(Rename(r1, ParseColumn("R1.id"), ParseColumn("R2.id")),
		ParseColumn("R1.name"), ParseColumn("name"))

	if !Equal(expected, actual) {
		t.Error("invalid rename result")
	}
}
