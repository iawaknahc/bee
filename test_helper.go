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

func testPanic(t *testing.T, op func(), value string) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("expected panic with %v", value)
		}
		s, ok := r.(string)
		if !ok {
			t.Errorf("expected panic with %v but got: %v", value, r)
		}
		if s != value {
			t.Errorf("expected panic with %v but got: %v", value, s)
		}
	}()
	op()
}
