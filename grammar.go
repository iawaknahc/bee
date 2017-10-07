package flexsql

import (
	"errors"
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

type JoinType uint

const (
	InnerJoin JoinType = iota
	LeftOuterJoin
	RightOuterJoin
	FullOuterJoin
)

type Node interface {
	Stringify(c *Compiler) error
	Transform(c *Compiler) Node
}

type Expr = Node

type CommaSeparated struct {
	nodes []Node
}

func (cs *CommaSeparated) Transform(c *Compiler) Node {
	for i, v := range cs.nodes {
		cs.nodes[i] = v.Transform(c)
	}
	return cs
}

func (cs *CommaSeparated) Stringify(c *Compiler) error {
	if len(cs.nodes) <= 0 {
		return nil
	}
	if err := cs.nodes[0].Stringify(c); err != nil {
		return err
	}
	for _, n := range cs.nodes[1:] {
		c.WriteVerbatim(",")
		if err := n.Stringify(c); err != nil {
			return err
		}
	}
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
	joinType JoinType
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
	switch j.joinType {
	case InnerJoin:
		c.WriteVerbatim(" JOIN ")
	case LeftOuterJoin:
		c.WriteVerbatim(" LEFT JOIN ")
	case RightOuterJoin:
		c.WriteVerbatim(" RIGHT JOIN ")
	case FullOuterJoin:
		c.WriteVerbatim(" FULL JOIN ")
	}
	if err := j.right.Stringify(c); err != nil {
		return err
	}
	c.WriteVerbatim(" ON ")
	return j.on.Stringify(c)
}

func Join(left, right *FromClauseItem, on Expr) *JoinClause {
	return &JoinClause{
		joinType: InnerJoin,
		left:     left,
		right:    right,
		on:       on,
	}
}

func LeftJoin(left, right *FromClauseItem, on Expr) *JoinClause {
	return &JoinClause{
		joinType: LeftOuterJoin,
		left:     left,
		right:    right,
		on:       on,
	}
}

func RightJoin(left, right *FromClauseItem, on Expr) *JoinClause {
	return &JoinClause{
		joinType: RightOuterJoin,
		left:     left,
		right:    right,
		on:       on,
	}
}

func FullJoin(left, right *FromClauseItem, on Expr) *JoinClause {
	return &JoinClause{
		joinType: FullOuterJoin,
		left:     left,
		right:    right,
		on:       on,
	}
}

type Quoted string

func (q Quoted) Transform(c *Compiler) Node {
	return q
}

func (q Quoted) Stringify(c *Compiler) error {
	c.WriteIdentifier(string(q))
	return nil
}

func Quote(name string) Quoted {
	return Quoted(name)
}

func Table(schema, tableName string) Node {
	if schema == "" {
		return Quote(tableName)
	}
	return dot(Quote(schema), Quote(tableName))
}

func Column(tableLabel, columnName string) Node {
	if tableLabel == "" {
		return Quote(columnName)

	}
	return dot(Quote(tableLabel), Quote(columnName))
}

type Labeled struct {
	expr        Expr
	quotedLabel Quoted
}

func (l *Labeled) Transform(c *Compiler) Node {
	l.expr = (l.expr.Transform(c)).(Expr)
	l.quotedLabel = (l.quotedLabel.Transform(c)).(Quoted)
	return l
}

func (l *Labeled) Stringify(c *Compiler) error {
	if err := l.expr.Stringify(c); err != nil {
		return err
	}
	c.WriteVerbatim(" ")
	return l.quotedLabel.Stringify(c)
}

func Label(expr Expr, label string) *Labeled {
	return &Labeled{
		expr:        expr,
		quotedLabel: Quote(label),
	}
}

func SelectExpr(sel *SelectStmt) Node {
	return &Paren{sel}
}

type LabeledSelectStmt struct {
	*Labeled
}

func (l *LabeledSelectStmt) Transform(c *Compiler) Node {
	l.Labeled = (l.Labeled.Transform(c)).(*Labeled)
	return l
}

func Subquery(sel *SelectStmt, alias string) *LabeledSelectStmt {
	return &LabeledSelectStmt{Label(&Paren{sel}, alias)}
}

type LabeledTable struct {
	*Labeled
}

func (l *LabeledTable) Transform(c *Compiler) Node {
	l.Labeled = (l.Labeled.Transform(c)).(*Labeled)
	return l
}

func TableRef(schema, tableName, label string) *LabeledTable {
	return &LabeledTable{Label(Table(schema, tableName), label)}
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

type Paren struct {
	Node Node
}

func (p *Paren) Transform(c *Compiler) Node {
	p.Node = p.Node.Transform(c)
	return p
}

func (p *Paren) Stringify(c *Compiler) error {
	c.WriteVerbatim("(")
	if err := p.Node.Stringify(c); err != nil {
		return err
	}
	c.WriteVerbatim(")")
	return nil
}

func Tuple(first Node, rest ...Node) Node {
	nodes := make([]Node, 1+len(rest))
	nodes[0] = first
	for i, e := range rest {
		nodes[i+1] = e
	}
	return &Paren{&CommaSeparated{nodes}}
}

type Placeholder string

func (p Placeholder) Transform(c *Compiler) Node {
	return p
}

func (p Placeholder) Stringify(c *Compiler) error {
	pos := c.insertPlaceholder(string(p))
	rendered := c.MakePlaceholder(string(p), pos)
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
	return Tuple(placeholders[0], exprs...), nil
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
	commaSeparated *CommaSeparated
}

func (g *GroupByClause) Transform(c *Compiler) Node {
	g.commaSeparated = (g.commaSeparated.Transform(c)).(*CommaSeparated)
	return g
}

func (g *GroupByClause) Stringify(c *Compiler) error {
	c.WriteVerbatim("GROUP BY ")
	return g.commaSeparated.Stringify(c)
}

func GroupBy(first Expr, rest ...Expr) *GroupByClause {
	nodes := make([]Node, len(rest)+1)
	nodes[0] = first
	for i, v := range rest {
		nodes[i+1] = v
	}
	return &GroupByClause{&CommaSeparated{nodes}}
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
	Columns       []*Labeled
	FromClause    *FromClause
	WhereClause   *WhereClause
	GroupByClause *GroupByClause
	HavingClause  *HavingClause
	LimitClause   *LimitClause
	OffsetClause  *OffsetClause
}

func (s *SelectStmt) Transform(c *Compiler) Node {
	for i, v := range s.Columns {
		s.Columns[i] = (v.Transform(c)).(*Labeled)
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
