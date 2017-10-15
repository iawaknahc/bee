package flexsql

import (
	"testing"
)

type literal string

func (l literal) Transform(c *Compiler) Node {
	return l
}

func (l literal) Stringify(c *Compiler) error {
	c.WriteVerbatim(string(l))
	return nil
}

func TestTable(t *testing.T) {
	f := &Table{
		Schema: "a",
		Name:   "b",
	}
	testCompile(t, f, `"a"."b"`)
}

func TestLabeledTable(t *testing.T) {
	f := &LabeledTable{
		Schema: "a",
		Name:   "b",
		Label:  "a_b",
	}
	testCompile(t, f, `"a"."b" "a_b"`)
}

func TestSubquery(t *testing.T) {
	sel := &SelectStmt{
		Columns: []*Labeled{Label(literal("1"), "one")},
	}
	s := Subquery(sel, "s")
	testCompile(t, s, `(SELECT 1 "one") "s"`)
}

func TestJoin(t *testing.T) {
	t1 := &LabeledTable{
		Schema: "s",
		Name:   "t1",
		Label:  "s_t1",
	}
	t2 := &LabeledTable{
		Schema: "s",
		Name:   "t2",
		Label:  "s_t2",
	}
	left := &FromClauseItem{
		TableRef: t1,
	}
	right := &FromClauseItem{
		TableRef: t2,
	}
	on := literal("1")
	cases := []compileTest{
		{Join(left, right, on), `"s"."t1" "s_t1" JOIN "s"."t2" "s_t2" ON 1`},
		{LeftJoin(left, right, on), `"s"."t1" "s_t1" LEFT JOIN "s"."t2" "s_t2" ON 1`},
		{RightJoin(left, right, on), `"s"."t1" "s_t1" RIGHT JOIN "s"."t2" "s_t2" ON 1`},
		{FullJoin(left, right, on), `"s"."t1" "s_t1" FULL JOIN "s"."t2" "s_t2" ON 1`},
	}
	testMany(t, cases)
}

func TestFrom(t *testing.T) {
	t1 := &FromClauseItem{
		TableRef: &LabeledTable{
			Schema: "s",
			Name:   "t1",
			Label:  "s_t1",
		},
	}
	t2 := &FromClauseItem{
		TableRef: &LabeledTable{
			Schema: "s",
			Name:   "t2",
			Label:  "s_t2",
		},
	}
	s := &FromClauseItem{
		Subquery: Subquery(&SelectStmt{
			Columns: []*Labeled{Label(literal("1"), "one")},
		}, "s"),
	}
	on := literal("TRUE")

	join1 := &FromClauseItem{
		JoinClause: Join(t1, t2, on),
	}
	join2 := &FromClauseItem{
		JoinClause: Join(join1, s, on),
	}
	from := &FromClause{join2}

	testCompile(t, from, `FROM "s"."t1" "s_t1" JOIN "s"."t2" "s_t2" ON TRUE JOIN (SELECT 1 "one") "s" ON TRUE`)
}

func TestWhere(t *testing.T) {
	f := literal("f")
	cases := []compileTest{
		{&WhereClause{f}, "WHERE f"},
	}
	testMany(t, cases)
}

func TestGroupBy(t *testing.T) {
	f := literal("f")
	g := literal("g")
	h := literal("h")
	cases := []compileTest{
		{GroupBy(f), "GROUP BY f"},
		{GroupBy(f, g), "GROUP BY f,g"},
		{GroupBy(f, g, h), "GROUP BY f,g,h"},
	}
	testMany(t, cases)
}

func TestSelectStmt(t *testing.T) {
	sel := &SelectStmt{
		Columns: []*Labeled{Label(literal("1"), "a")},
	}
	testCompile(t, sel, `SELECT 1 "a"`)

	sel.FromClause = &FromClause{&FromClauseItem{
		TableRef: &LabeledTable{
			Schema: "s",
			Name:   "a",
			Label:  "s_a",
		},
	}}
	testCompile(t, sel, `SELECT 1 "a" FROM "s"."a" "s_a"`)

	sel.WhereClause = &WhereClause{literal("TRUE")}
	testCompile(t, sel, `SELECT 1 "a" FROM "s"."a" "s_a" WHERE TRUE`)

	sel.GroupByClause = GroupBy(literal("f"))
	testCompile(t, sel, `SELECT 1 "a" FROM "s"."a" "s_a" WHERE TRUE GROUP BY f`)

	sel.HavingClause = &HavingClause{literal("FALSE")}
	testCompile(t, sel, `SELECT 1 "a" FROM "s"."a" "s_a" WHERE TRUE GROUP BY f HAVING FALSE`)

	sel.LimitClause = &LimitClause{literal("10")}
	testCompile(t, sel, `SELECT 1 "a" FROM "s"."a" "s_a" WHERE TRUE GROUP BY f HAVING FALSE LIMIT 10`)

	sel.OffsetClause = &OffsetClause{literal("20")}
	testCompile(t, sel, `SELECT 1 "a" FROM "s"."a" "s_a" WHERE TRUE GROUP BY f HAVING FALSE LIMIT 10 OFFSET 20`)
}

func TestLabel(t *testing.T) {
	f := literal("f")
	cases := []compileTest{
		{Label(f, "g"), `f "g"`},
	}
	testMany(t, cases)
}

func TestQuote(t *testing.T) {
	cases := []compileTest{
		{Quote("a"), `"a"`},
		{Quote("1a"), `"1a"`},
		{Quote("日本語"), `U&"\+0065E5\+00672C\+008A9E"`},
	}
	testMany(t, cases)
}

func TestTuple(t *testing.T) {
	f := literal("f")
	cases := []compileTest{
		{Tuple(f), "(f)"},
		{Tuple(f, f), "(f,f)"},
	}
	testMany(t, cases)
}

func TestPlaceholders(t *testing.T) {
	cases := []struct {
		prefix       string
		length       int
		out          string
		placeholders []string
	}{
		{
			"a",
			2,
			"($1,$2)",
			[]string{"a1", "a2"},
		},
	}
	for _, case_ := range cases {
		c := &Compiler{
			dialect: &Postgres{},
		}
		placeholders, tuple, err := PlaceholderTuple(case_.prefix, case_.length)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		actual, err := c.Compile(tuple)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		testEqual(t, actual, case_.out)
		if len(placeholders) != len(case_.placeholders) {
			t.Errorf("unmatched length")
		}
		for i, p := range placeholders {
			actual := string(p)
			expected := case_.placeholders[i]
			testEqual(t, actual, expected)
		}
	}
}

func TestFunc(t *testing.T) {
	f := literal("f")
	cases := []compileTest{
		{Func("foobar")(), "foobar()"},
		{Func("foobar")(f), "foobar(f)"},
		{Func("foobar")(f, f), "foobar(f,f)"},
		{Func("a.b")(f, f), "a.b(f,f)"},
		{Func0("foobar"), "foobar"},
	}
	testMany(t, cases)
}

func TestFuncPanic(t *testing.T) {
	testPanic(t, func() { Func("1") }, "illegal function name: 1")
	testPanic(t, func() { Func("a-") }, "illegal function name: a-")
}
