package flexsql

import (
	"testing"
)

func TestUnaryOperator(t *testing.T) {
	a := literal("a")
	cases := []compileTest{
		{Not(IsNull(a)), "a IS NOT NULL"},
		{Not(IsNotNull(a)), "a IS NULL"},
		{Not(IsTrue(a)), "a IS NOT TRUE"},
		{Not(IsNotTrue(a)), "a IS TRUE"},
		{Not(IsFalse(a)), "a IS NOT FALSE"},
		{Not(IsNotFalse(a)), "a IS FALSE"},
		{Not(Not(a)), "a"},
		{Not(Eq(a, a)), "a <> a"},
		{Not(NotEq(a, a)), "a = a"},

		{Not(Not(IsNull(a))), "a IS NULL"},
		{Not(Not(IsNotNull(a))), "a IS NOT NULL"},
		{Not(Not(IsTrue(a))), "a IS TRUE"},
		{Not(Not(IsNotTrue(a))), "a IS NOT TRUE"},
		{Not(Not(IsFalse(a))), "a IS FALSE"},
		{Not(Not(IsNotFalse(a))), "a IS NOT FALSE"},
		{Not(Not(Not(a))), "NOT a"},
		{Not(Not(Eq(a, a))), "a = a"},
		{Not(Not(NotEq(a, a))), "a <> a"},

		{IsNull(Not(a)), "(NOT a) IS NULL"},
		{IsNull(Not(Not(a))), "a IS NULL"},
		{Not(IsNull(Not(Not(a)))), "a IS NOT NULL"},
		{Not(IsNull(Not(a))), "(NOT a) IS NOT NULL"},
	}
	testMany(t, cases)
}

func TestBinaryOperator(t *testing.T) {
	f := literal("f")
	cases := []compileTest{
		{dot(f, f), "f.f"},
		{Sub(f, f), "f - f"},
		{Sub(f, Sub(f, f)), "f - (f - f)"},
		{Sub(Sub(f, f), f), "f - f - f"},
		{Div(f, Sub(f, f)), "f / (f - f)"},
		{Sub(Div(f, f), f), "f / f - f"},
		{Sub(f, Div(f, f)), "f - f / f"},
		{Div(Sub(f, f), f), "(f - f) / f"},
		{Sub(Div(Sub(f, f), f), f), "(f - f) / f - f"},
		{Div(Sub(f, f), Sub(f, f)), "(f - f) / (f - f)"},
		{Sub(Div(f, f), Div(f, f)), "f / f - f / f"},
		{Sub(Div(f, f), Sub(f, f)), "f / f - (f - f)"},
		{Div(Div(f, f), Sub(f, f)), "f / f / (f - f)"},
		{Div(f, Div(f, Sub(f, f))), "f / (f / (f - f))"},
		{Eq(Eq(f, f), f), "(f = f) = f"},
		{Eq(f, Eq(f, f)), "f = (f = f)"},
	}
	testMany(t, cases)
}

func TestOperator(t *testing.T) {
	f := literal("f")
	cases := []compileTest{
		{Not(Add(f, f)), "NOT f + f"},
		{Add(f, Not(f)), "f + (NOT f)"},
		{Add(Not(f), f), "(NOT f) + f"},
	}
	testMany(t, cases)
}

func TestLike(t *testing.T) {
	f := literal("f")
	cases := []compileTest{
		{Like(f, f), "f LIKE f"},
		{NotLike(f, f), "f NOT LIKE f"},
		{ILike(f, f), "f ILIKE f"},
		{NotILike(f, f), "f NOT ILIKE f"},

		{Not(Like(f, f)), "f NOT LIKE f"},
		{Not(NotLike(f, f)), "f LIKE f"},
		{Not(ILike(f, f)), "f NOT ILIKE f"},
		{Not(NotILike(f, f)), "f ILIKE f"},

		{Like(f, Like(f, f)), "f LIKE (f LIKE f)"},
		{Like(Like(f, f), f), "(f LIKE f) LIKE f"},
		{Like(Not(f), f), "(NOT f) LIKE f"},
		{Like(f, Not(f)), "f LIKE (NOT f)"},
	}
	testMany(t, cases)
}

func TestIn(t *testing.T) {
	f := literal("f")
	cases := []compileTest{
		{In(f, Tuple(f)), "f IN (f)"},
		{Not(In(f, Tuple(f))), "f NOT IN (f)"},
		{Not(Not(In(f, Tuple(f)))), "f IN (f)"},

		{NotIn(f, Tuple(f)), "f NOT IN (f)"},
		{Not(NotIn(f, Tuple(f))), "f IN (f)"},
		{Not(Not(NotIn(f, Tuple(f)))), "f NOT IN (f)"},

		{In(In(f, Tuple(f)), Tuple(f)), "(f IN (f)) IN (f)"},
	}
	testMany(t, cases)
}

func TestBetween(t *testing.T) {
	f := literal("f")
	cases := []compileTest{
		{Between(f, f, f), "f BETWEEN f AND f"},
		{Not(Between(f, f, f)), "f NOT BETWEEN f AND f"},
		{Not(Not(Between(f, f, f))), "f BETWEEN f AND f"},

		{NotBetween(f, f, f), "f NOT BETWEEN f AND f"},
		{Not(NotBetween(f, f, f)), "f BETWEEN f AND f"},
		{Not(Not(NotBetween(f, f, f))), "f NOT BETWEEN f AND f"},

		{Between(Between(f, f, f), f, f), "(f BETWEEN f AND f) BETWEEN f AND f"},
		{Between(f, Between(f, f, f), f), "f BETWEEN (f BETWEEN f AND f) AND f"},
		{Between(f, f, Between(f, f, f)), "f BETWEEN f AND (f BETWEEN f AND f)"},
	}
	testMany(t, cases)
}
