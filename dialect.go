package flexsql

type Dialect interface {
	QuoteIdentifier(i string) string
	MakePlaceholder(name string, position uint) string
	Precedence(op OperatorType) uint
	Associativity(op OperatorType) Associativity
}
