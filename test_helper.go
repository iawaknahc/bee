package flexsql

import (
	"reflect"
	"testing"
)

type compileTest struct {
	in  Node
	out string
}

func testEqual(t *testing.T, actual, expected interface{}) {
	if actual != expected {
		t.Errorf("expected: %v but got: %v", expected, actual)
	}
}

func testDeepEqual(t *testing.T, actual, expected interface{}) {
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("expected: %v but got: %v", expected, actual)
	}
}

func testMany(t *testing.T, cases []compileTest) {
	for _, case_ := range cases {
		testCompile(t, case_.in, case_.out)
	}
}

func testCompile(t *testing.T, e Node, expected string) {
	c := &Compiler{
		dialect: &Postgres{},
	}
	actual, err := c.Compile(e)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	testEqual(t, actual, expected)
}
