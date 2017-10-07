package flexsql

import (
	"testing"
)

func TestBuildParams(t *testing.T) {
	a := Placeholder("a")
	b := Placeholder("b")

	cases := []struct {
		in           Expr
		out          string
		inputParams  map[string]interface{}
		outputParams []interface{}
	}{
		{
			And(Eq(a, b), NotEq(b, a)),
			"$1 = $2 AND $3 <> $4",
			map[string]interface{}{
				"a": 1,
				"b": 2,
			},
			[]interface{}{1, 2, 2, 1},
		},
	}

	for _, case_ := range cases {
		c := &Compiler{
			dialect: &Postgres{},
		}
		out, err := c.Compile(case_.in)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		testEqual(t, out, case_.out)
		params, err := c.BuildParams(case_.inputParams)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		testDeepEqual(t, params, case_.outputParams)
	}
}
