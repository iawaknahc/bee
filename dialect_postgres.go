package flexsql

import (
	"bytes"
	"fmt"
)

type Postgres struct{}

func (p *Postgres) isLegalFirstIdentifierCharacter(r rune) bool {
	if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r == '_' {
		return true
	}
	return false
}

func (p *Postgres) isLegalIdentifierCharacter(r rune) bool {
	b := p.isLegalFirstIdentifierCharacter(r)
	if b {
		return true
	}
	if r >= '0' && r <= '9' {
		return true
	}
	return false
}

func (p *Postgres) unicodeEscapeRune(r rune) string {
	return fmt.Sprintf("\\+%06X", int32(r))
}

func (p *Postgres) QuoteIdentifier(i string) string {
	var buffer bytes.Buffer
	needPrefix := false

	buffer.WriteString(`"`)
	for _, runeValue := range i {
		needUnicodeEscape := false
		if !p.isLegalIdentifierCharacter(runeValue) {
			needPrefix = true
			needUnicodeEscape = true
		}
		if needUnicodeEscape {
			buffer.WriteString(p.unicodeEscapeRune(runeValue))
		} else {
			buffer.WriteRune(runeValue)
		}
	}
	buffer.WriteString(`"`)

	s := buffer.String()
	if !needPrefix {
		return s
	}
	return "U&" + s
}

func (p *Postgres) MakePlaceholder(name string, position uint) string {
	return fmt.Sprintf("$%d", position+1)
}

func (p *Postgres) Precedence(op OperatorType) uint {
	switch op {
	case OpOr:
		return 1
	case OpAnd:
		return 2
	case OpNot:
		return 3
	case OpIsNull, OpIsNotNull, OpIsTrue, OpIsNotTrue, OpIsFalse, OpIsNotFalse:
		return 4
	case OpLt, OpGt, OpEq, OpLte, OpGte, OpNotEq:
		return 5
	case OpBetween, OpNotBetween, OpIn, OpNotIn, OpLike, OpNotLike, OpILike, OpNotILike:
		return 6
	case OpAdd, OpSub:
		return 8
	case OpMul, OpDiv, OpMod:
		return 9
	}
	return 0
}

func (p *Postgres) Associativity(op OperatorType) Associativity {
	switch op {
	case OpIsNull, OpIsNotNull, OpIsTrue, OpIsNotTrue, OpIsFalse, OpIsNotFalse, OpOr, OpAnd, OpAdd, OpSub, OpMul, OpDiv, OpMod:
		return LeftAssociative
	case OpNot:
		return RightAssociative
	case OpLt, OpGt, OpEq, OpLte, OpGte, OpNotEq, OpBetween, OpNotBetween, OpIn, OpNotIn, OpLike, OpNotLike, OpILike, OpNotILike:
		return NonAssociative
	}
	return 0
}
