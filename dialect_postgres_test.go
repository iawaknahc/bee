package flexsql

import (
	"testing"
)

func TestQuoteIdentifier(t *testing.T) {
	p := Postgres{}
	cases := [][]string{
		{"a", `"a"`},
		{"1a", `"1a"`},
		{"日本語", `U&"\+0065E5\+00672C\+008A9E"`},
	}
	for _, case_ := range cases {
		input := case_[0]
		expected := case_[1]
		actual := p.QuoteIdentifier(input)
		testEqual(t, actual, expected)
	}
}

func TestMakePlaceholder(t *testing.T) {
	p := Postgres{}
	testEqual(t, p.MakePlaceholder("unimportant", 0), "$1")
}
