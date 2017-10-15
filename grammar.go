package flexsql

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

var (
	ErrNoPrecedence          = errors.New("No precedence")
	ErrNoAssociativity       = errors.New("No associativity")
	ErrUnknownFromClauseItem = errors.New("Unknown FromClauseItem")
	ErrNonAssociative        = errors.New("Not associative")
	ErrZeroLength            = errors.New("Zero length")
	ErrUnknownInputKey       = errors.New("Unknown input key")
	ErrUnboundPlaceholder    = errors.New("Unbound placeholder")
)

var funcNameRegexp = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_.]*$`)

type Node interface {
	Stringify(c *Compiler) error
	Transform(c *Compiler) Node
}

type Expr = Node

type FuncExpr struct {
	name            string
	args            []Expr
	omitParentheses bool
}

func checkFuncName(name string) {
	re := funcNameRegexp.Copy()
	if !re.MatchString(name) {
		panic(fmt.Sprintf("illegal function name: %v", name))
	}
}

func Func(name string) func(...Expr) *FuncExpr {
	checkFuncName(name)
	return func(args ...Expr) *FuncExpr {
		return &FuncExpr{
			name: name,
			args: args,
		}
	}
}

func Func0(name string) *FuncExpr {
	checkFuncName(name)
	return &FuncExpr{
		name:            name,
		omitParentheses: true,
	}
}

func (f *FuncExpr) Transform(c *Compiler) Node {
	for i, v := range f.args {
		f.args[i] = v.Transform(c).(Expr)
	}
	return f
}

func (f *FuncExpr) Stringify(c *Compiler) error {
	c.WriteVerbatim(f.name)
	if len(f.args) <= 0 {
		if f.omitParentheses {
			return nil
		}
		c.WriteVerbatim("()")
		return nil
	}
	c.WriteVerbatim("(")
	if err := f.args[0].Stringify(c); err != nil {
		return err
	}
	for _, e := range f.args[1:] {
		c.WriteVerbatim(",")
		if err := e.Stringify(c); err != nil {
			return err
		}
	}
	c.WriteVerbatim(")")
	return nil
}

func stringifyCommaSeparated(nodes []Node, c *Compiler) error {
	if len(nodes) <= 0 {
		return nil
	}
	if err := nodes[0].Stringify(c); err != nil {
		return err
	}
	for _, n := range nodes[1:] {
		c.WriteVerbatim(",")
		if err := n.Stringify(c); err != nil {
			return err
		}
	}
	return nil
}

func stringifyParen(node Node, c *Compiler) error {
	c.WriteVerbatim("(")
	if err := node.Stringify(c); err != nil {
		return err
	}
	c.WriteVerbatim(")")
	return nil
}

type FromClause struct {
	FromClauseItem *FromClauseItem
}

func (f *FromClause) Transform(c *Compiler) Node {
	f.FromClauseItem = (f.FromClauseItem.Transform(c)).(*FromClauseItem)
	return f
}

func (f *FromClause) Stringify(c *Compiler) error {
	c.WriteVerbatim("FROM ")
	return f.FromClauseItem.Stringify(c)
}

type JoinClause struct {
	joinType string
	left     *FromClauseItem
	right    *FromClauseItem
	on       Expr
}

func (j *JoinClause) Transform(c *Compiler) Node {
	j.left = (j.left.Transform(c)).(*FromClauseItem)
	j.right = (j.right.Transform(c)).(*FromClauseItem)
	j.on = (j.on.Transform(c)).(Expr)
	return j
}

func (j *JoinClause) Stringify(c *Compiler) error {
	if err := j.left.Stringify(c); err != nil {
		return err
	}
	c.WriteVerbatim(" " + j.joinType + " ")
	if err := j.right.Stringify(c); err != nil {
		return err
	}
	c.WriteVerbatim(" ON ")
	return j.on.Stringify(c)
}

func Join(left, right *FromClauseItem, on Expr) *JoinClause {
	return &JoinClause{
		joinType: "JOIN",
		left:     left,
		right:    right,
		on:       on,
	}
}

func LeftJoin(left, right *FromClauseItem, on Expr) *JoinClause {
	return &JoinClause{
		joinType: "LEFT JOIN",
		left:     left,
		right:    right,
		on:       on,
	}
}

func RightJoin(left, right *FromClauseItem, on Expr) *JoinClause {
	return &JoinClause{
		joinType: "RIGHT JOIN",
		left:     left,
		right:    right,
		on:       on,
	}
}

func FullJoin(left, right *FromClauseItem, on Expr) *JoinClause {
	return &JoinClause{
		joinType: "FULL JOIN",
		left:     left,
		right:    right,
		on:       on,
	}
}

type LabeledSelectStmt struct {
	SelectStmt *SelectStmt
	Label      string
}

func (l *LabeledSelectStmt) Transform(c *Compiler) Node {
	l.SelectStmt = (l.SelectStmt.Transform(c)).(*SelectStmt)
	return l
}

func (l *LabeledSelectStmt) Stringify(c *Compiler) error {
	c.WriteVerbatim("(")
	if err := l.SelectStmt.Stringify(c); err != nil {
		return err
	}
	c.WriteVerbatim(") ")
	c.WriteIdentifier(l.Label)
	return nil
}

type Column struct {
	TableLabel string
	Name       string
}

func (col *Column) Transform(c *Compiler) Node {
	return col
}

func (col *Column) Stringify(c *Compiler) error {
	if col.TableLabel != "" {
		c.WriteIdentifier(col.TableLabel)
		c.WriteVerbatim(".")
	}
	c.WriteIdentifier(col.Name)
	return nil
}

type LabeledColumn struct {
	Expr  Expr
	Label string
}

func (l *LabeledColumn) Transform(c *Compiler) Node {
	return l
}

func (l *LabeledColumn) Stringify(c *Compiler) error {
	if err := l.Expr.Stringify(c); err != nil {
		return err
	}
	c.WriteVerbatim(" ")
	c.WriteIdentifier(l.Label)
	return nil
}

type Table struct {
	Schema string
	Name   string
}

func (t *Table) Transform(c *Compiler) Node {
	return t
}

func (t *Table) Stringify(c *Compiler) error {
	if t.Schema != "" {
		c.WriteIdentifier(t.Schema)
		c.WriteVerbatim(".")
	}
	c.WriteIdentifier(t.Name)
	return nil
}

type LabeledTable struct {
	Schema string
	Name   string
	Label  string
}

func (l *LabeledTable) Transform(c *Compiler) Node {
	return l
}

func (l *LabeledTable) Stringify(c *Compiler) error {
	if l.Schema != "" {
		c.WriteIdentifier(l.Schema)
		c.WriteVerbatim(".")
	}
	c.WriteIdentifier(l.Name)
	c.WriteVerbatim(" ")
	c.WriteIdentifier(l.Label)
	return nil
}

type FromClauseItem struct {
	TableRef   *LabeledTable
	Subquery   *LabeledSelectStmt
	JoinClause *JoinClause
}

func (f *FromClauseItem) Transform(c *Compiler) Node {
	if f.TableRef != nil {
		f.TableRef = (f.TableRef.Transform(c)).(*LabeledTable)
	} else if f.Subquery != nil {
		f.Subquery = (f.Subquery.Transform(c)).(*LabeledSelectStmt)
	} else if f.JoinClause != nil {
		f.JoinClause = (f.JoinClause.Transform(c)).(*JoinClause)
	}
	return f
}

func (f *FromClauseItem) Stringify(c *Compiler) error {
	if f.TableRef != nil {
		return f.TableRef.Stringify(c)
	} else if f.Subquery != nil {
		return f.Subquery.Stringify(c)
	} else if f.JoinClause != nil {
		return f.JoinClause.Stringify(c)
	}
	return ErrUnknownFromClauseItem
}

type Tuple struct {
	Exprs []Expr
}

func (t *Tuple) Transform(c *Compiler) Node {
	for i, e := range t.Exprs {
		t.Exprs[i] = e.Transform(c).(Expr)
	}
	return t
}

func (t *Tuple) Stringify(c *Compiler) error {
	c.WriteVerbatim("(")
	if err := stringifyCommaSeparated(t.Exprs, c); err != nil {
		return err
	}
	c.WriteVerbatim(")")
	return nil
}

func MakeTuple(first Expr, rest ...Expr) *Tuple {
	exprs := make([]Expr, 1+len(rest))
	exprs[0] = first
	for i, e := range rest {
		exprs[i+1] = e
	}
	return &Tuple{exprs}
}

type Placeholder string

func (p Placeholder) Transform(c *Compiler) Node {
	return p
}

func (p Placeholder) Stringify(c *Compiler) error {
	pos := c.insertPlaceholder(string(p))
	rendered := c.makePlaceholder(string(p), pos)
	c.WriteVerbatim(rendered)
	return nil
}

func generatePlaceholders(prefix string, length int) ([]Placeholder, error) {
	if length <= 0 {
		return nil, ErrZeroLength
	}
	output := make([]Placeholder, length)
	for i := 0; i < length; i++ {
		output[i] = Placeholder(prefix + strconv.Itoa(i+1))
	}
	return output, nil
}

func makePlaceholderTuple(placeholders []Placeholder) (Node, error) {
	if len(placeholders) <= 0 {
		return nil, ErrZeroLength
	}
	exprs := make([]Expr, len(placeholders[1:]))
	for i, v := range placeholders[1:] {
		exprs[i] = v
	}
	return MakeTuple(placeholders[0], exprs...), nil
}

func PlaceholderTuple(prefix string, length int) ([]Placeholder, Node, error) {
	placeholders, err := generatePlaceholders(prefix, length)
	if err != nil {
		return nil, nil, err
	}
	tuple, err := makePlaceholderTuple(placeholders)
	if err != nil {
		return nil, nil, err
	}
	return placeholders, tuple, err
}

type WhereClause struct {
	Expr Expr
}

func (w *WhereClause) Transform(c *Compiler) Node {
	w.Expr = w.Expr.Transform(c).(Expr)
	return w
}

func (w *WhereClause) Stringify(c *Compiler) error {
	c.WriteVerbatim("WHERE ")
	return w.Expr.Stringify(c)
}

type GroupByClause struct {
	Exprs []Expr
}

func (g *GroupByClause) Transform(c *Compiler) Node {
	for i, e := range g.Exprs {
		g.Exprs[i] = e.Transform(c).(Expr)
	}
	return g
}

func (g *GroupByClause) Stringify(c *Compiler) error {
	c.WriteVerbatim("GROUP BY ")
	return stringifyCommaSeparated(g.Exprs, c)
}

func GroupBy(first Expr, rest ...Expr) *GroupByClause {
	exprs := make([]Expr, len(rest)+1)
	exprs[0] = first
	for i, v := range rest {
		exprs[i+1] = v
	}
	return &GroupByClause{exprs}
}

type HavingClause struct {
	Expr Expr
}

func (h *HavingClause) Transform(c *Compiler) Node {
	h.Expr = (h.Expr.Transform(c)).(Expr)
	return h
}

func (h *HavingClause) Stringify(c *Compiler) error {
	c.WriteVerbatim("HAVING ")
	return h.Expr.Stringify(c)
}

type LimitClause struct {
	Expr Expr
}

func (l *LimitClause) Transform(c *Compiler) Node {
	l.Expr = (l.Expr.Transform(c)).(Expr)
	return l
}

func (l *LimitClause) Stringify(c *Compiler) error {
	c.WriteVerbatim("LIMIT ")
	return l.Expr.Stringify(c)
}

type OffsetClause struct {
	Expr Expr
}

func (o *OffsetClause) Transform(c *Compiler) Node {
	o.Expr = (o.Expr.Transform(c)).(Expr)
	return o
}

func (o *OffsetClause) Stringify(c *Compiler) error {
	c.WriteVerbatim("OFFSET ")
	return o.Expr.Stringify(c)
}

type SelectStmt struct {
	Columns       []*LabeledColumn
	FromClause    *FromClause
	WhereClause   *WhereClause
	GroupByClause *GroupByClause
	HavingClause  *HavingClause
	LimitClause   *LimitClause
	OffsetClause  *OffsetClause
}

func (s *SelectStmt) Transform(c *Compiler) Node {
	for i, v := range s.Columns {
		s.Columns[i] = (v.Transform(c)).(*LabeledColumn)
	}
	if s.FromClause != nil {
		s.FromClause = (s.FromClause.Transform(c)).(*FromClause)
	}
	if s.WhereClause != nil {
		s.WhereClause = (s.WhereClause.Transform(c)).(*WhereClause)
	}
	if s.GroupByClause != nil {
		s.GroupByClause = (s.GroupByClause.Transform(c)).(*GroupByClause)
	}
	if s.HavingClause != nil {
		s.HavingClause = (s.HavingClause.Transform(c)).(*HavingClause)
	}
	if s.LimitClause != nil {
		s.LimitClause = (s.LimitClause.Transform(c)).(*LimitClause)
	}
	if s.OffsetClause != nil {
		s.OffsetClause = (s.OffsetClause.Transform(c)).(*OffsetClause)
	}
	return s
}

func (s *SelectStmt) Stringify(c *Compiler) error {
	c.WriteVerbatim("SELECT ")
	if err := s.Columns[0].Stringify(c); err != nil {
		return err
	}
	for _, se := range s.Columns[1:] {
		c.WriteVerbatim(",")
		if err := se.Stringify(c); err != nil {
			return err
		}
	}
	if s.FromClause != nil {
		c.WriteVerbatim(" ")
		if err := s.FromClause.Stringify(c); err != nil {
			return err
		}
	}
	if s.WhereClause != nil {
		c.WriteVerbatim(" ")
		if err := s.WhereClause.Stringify(c); err != nil {
			return err
		}
	}
	if s.GroupByClause != nil {
		c.WriteVerbatim(" ")
		if err := s.GroupByClause.Stringify(c); err != nil {
			return err
		}
	}
	if s.HavingClause != nil {
		c.WriteVerbatim(" ")
		if err := s.HavingClause.Stringify(c); err != nil {
			return err
		}
	}
	if s.LimitClause != nil {
		c.WriteVerbatim(" ")
		if err := s.LimitClause.Stringify(c); err != nil {
			return err
		}
	}
	if s.OffsetClause != nil {
		c.WriteVerbatim(" ")
		if err := s.OffsetClause.Stringify(c); err != nil {
			return err
		}
	}
	return nil
}
