package relatalg

import "testing"

func TestCartesian(t *testing.T) {
	r1Chan := make(chan Row, 2)
	r2Chan := make(chan Row, 3)
	r1Chan <- Row{ParseColumn("R1.id"): 0, ParseColumn("R1.name"): "Bob"}
	r1Chan <- Row{ParseColumn("R1.id"): 1, ParseColumn("R1.name"): "Joe"}
	r2Chan <- Row{ParseColumn("R2.id"): 0, ParseColumn("R2.name"): "Stacey"}
	r2Chan <- Row{ParseColumn("R2.id"): 1, ParseColumn("R2.name"): "Jill"}
	r2Chan <- Row{ParseColumn("R2.id"): 2, ParseColumn("R2.name"): "Amber"}
	close(r1Chan)
	close(r2Chan)
	r1 := &ConcreteRelation{
		S: map[Column]Type{ParseColumn("R1.id"): Integer, ParseColumn("R1.name"): String},
		E: r1Chan,
	}
	r2 := &ConcreteRelation{
		S: map[Column]Type{ParseColumn("R2.id"): Integer, ParseColumn("R2.name"): String},
		E: r2Chan,
	}
	actual := Cartesian(r1, r2)

	expChan := make(chan Row, 6)
	expChan <- Row{
		ParseColumn("R1.id"): 0, ParseColumn("R1.name"): "Bob",
		ParseColumn("R2.id"): 0, ParseColumn("R2.name"): "Stacey",
	}
	expChan <- Row{
		ParseColumn("R1.id"): 0, ParseColumn("R1.name"): "Bob",
		ParseColumn("R2.id"): 1, ParseColumn("R2.name"): "Jill",
	}
	expChan <- Row{
		ParseColumn("R1.id"): 0, ParseColumn("R1.name"): "Bob",
		ParseColumn("R2.id"): 2, ParseColumn("R2.name"): "Amber",
	}
	expChan <- Row{
		ParseColumn("R1.id"): 1, ParseColumn("R1.name"): "Joe",
		ParseColumn("R2.id"): 0, ParseColumn("R2.name"): "Stacey",
	}
	expChan <- Row{
		ParseColumn("R1.id"): 1, ParseColumn("R1.name"): "Joe",
		ParseColumn("R2.id"): 1, ParseColumn("R2.name"): "Jill",
	}
	expChan <- Row{
		ParseColumn("R1.id"): 1, ParseColumn("R1.name"): "Joe",
		ParseColumn("R2.id"): 2, ParseColumn("R2.name"): "Amber",
	}
	close(expChan)
	expected := &ConcreteRelation{
		S: map[Column]Type{
			ParseColumn("R1.id"): Integer, ParseColumn("R1.name"): String,
			ParseColumn("R2.id"): Integer, ParseColumn("R2.name"): String,
		},
		E: expChan,
	}

	if !Equal(expected, actual) {
		t.Errorf("invalid joined relation")
	}
}
