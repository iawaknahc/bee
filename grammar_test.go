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

func TestColumn(t *testing.T) {
	cases := []compileTest{
		{&Column{"t", "a"}, `"t"."a"`},
		{&Column{"", "a"}, `"a"`},
	}
	testMany(t, cases)
}

func TestLabeledColumn(t *testing.T) {
	cases := []compileTest{
		{&LabeledColumn{&Column{"t", "a"}, "t_a"}, `"t"."a" "t_a"`},
		{&LabeledColumn{literal("1"), "f"}, `1 "f"`},
	}
	testMany(t, cases)
}

func TestTable(t *testing.T) {
	cases := []compileTest{
		{&Table{"a", "b"}, `"a"."b"`},
		{&Table{"", "b"}, `"b"`},
	}
	testMany(t, cases)
}

func TestLabeledTable(t *testing.T) {
	cases := []compileTest{
		{&LabeledTable{Schema: "a", Name: "b", Label: "a_b"}, `"a"."b" "a_b"`},
		{&LabeledTable{Name: "b", Label: "a_b"}, `"b" "a_b"`},
	}
	testMany(t, cases)
}

func TestSubquery(t *testing.T) {
	sel := &SelectStmt{
		Columns: []*LabeledColumn{
			&LabeledColumn{literal("1"), "one"},
		},
	}
	s := &LabeledSelectStmt{sel, "s"}
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
		Subquery: &LabeledSelectStmt{
			&SelectStmt{
				Columns: []*LabeledColumn{
					&LabeledColumn{literal("1"), "one"},
				},
			},
			"s",
		},
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
		Columns: []*LabeledColumn{
			&LabeledColumn{literal("1"), "a"},
		},
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

	sel.OrderByClause = OrderBy(Asc(literal("f")))
	testCompile(t, sel, `SELECT 1 "a" FROM "s"."a" "s_a" WHERE TRUE GROUP BY f HAVING FALSE ORDER BY f`)

	sel.LimitClause = &LimitClause{literal("10")}
	testCompile(t, sel, `SELECT 1 "a" FROM "s"."a" "s_a" WHERE TRUE GROUP BY f HAVING FALSE ORDER BY f LIMIT 10`)

	sel.OffsetClause = &OffsetClause{literal("20")}
	testCompile(t, sel, `SELECT 1 "a" FROM "s"."a" "s_a" WHERE TRUE GROUP BY f HAVING FALSE ORDER BY f LIMIT 10 OFFSET 20`)
}

func TestTuple(t *testing.T) {
	f := literal("f")
	cases := []compileTest{
		{MakeTuple(f), "(f)"},
		{MakeTuple(f, f), "(f,f)"},
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

func TestOrderByItem(t *testing.T) {
	f := literal("f")
	cases := []compileTest{
		{Asc(f), "f"},
		{Desc(f), "f DESC"},
		{NullsFirst(Asc(f)), "f NULLS FIRST"},
		{NullsLast(Asc(f)), "f"},
		{NullsFirst(Desc(f)), "f DESC"},
		{NullsLast(Desc(f)), "f DESC NULLS LAST"},
	}
	testMany(t, cases)
}

func TestOrderByClause(t *testing.T) {
	f := literal("f")
	cases := []compileTest{
		{OrderBy(Asc(f)), "ORDER BY f"},
		{OrderBy(Asc(f), Desc(f)), "ORDER BY f,f DESC"},
	}
	testMany(t, cases)
}

func TestCaseExpr(t *testing.T) {
	a := literal("a")
	b := literal("b")
	c := literal("c")
	d := literal("d")
	e := literal("e")
	cases := []compileTest{
		{Case(a, b), "CASE WHEN a THEN b END"},
		{Case(a, b).When(c, d), "CASE WHEN a THEN b WHEN c THEN d END"},
		{Case(a, b).When(c, d).Else(e), "CASE WHEN a THEN b WHEN c THEN d ELSE e END"},
	}
	testMany(t, cases)
}
