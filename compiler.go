package flexsql

import (
	"bytes"
)

type Compiler struct {
	dialect             Dialect
	buffer              *bytes.Buffer
	placeholderPosition uint
	positionToName      map[uint]string
	nameToPositions     map[string][]uint
}

func (c *Compiler) precedence(op OperatorType) uint {
	return c.dialect.Precedence(op)
}

func (c *Compiler) associativity(op OperatorType) Associativity {
	return c.dialect.Associativity(op)
}

func (c *Compiler) makePlaceholder(name string, position uint) string {
	return c.dialect.MakePlaceholder(name, position)
}

func (c *Compiler) WriteVerbatim(s string) {
	if c.buffer == nil {
		c.buffer = &bytes.Buffer{}
	}
	c.buffer.WriteString(s)
}

func (c *Compiler) WriteIdentifier(i string) {
	c.WriteVerbatim(c.dialect.QuoteIdentifier(i))
}

func (c *Compiler) insertPlaceholder(name string) uint {
	pos := c.placeholderPosition
	c.placeholderPosition += 1

	if c.positionToName == nil {
		c.positionToName = make(map[uint]string)
	}
	c.positionToName[pos] = name

	if c.nameToPositions == nil {
		c.nameToPositions = make(map[string][]uint)
	}
	c.nameToPositions[name] = append(c.nameToPositions[name], pos)

	return pos
}

func (c *Compiler) Compile(e Node) (string, error) {
	c.buffer = &bytes.Buffer{}
	c.placeholderPosition = 0
	c.positionToName = make(map[uint]string)
	c.nameToPositions = make(map[string][]uint)
	if err := e.Transform(c).Stringify(c); err != nil {
		return "", err
	}
	return c.buffer.String(), nil
}

func (c *Compiler) BuildParams(input map[string]interface{}) ([]interface{}, error) {
	consumedLength := 0
	output := make([]interface{}, c.placeholderPosition)

	for k, v := range input {
		positions, ok := c.nameToPositions[k]
		if !ok {
			return nil, ErrUnknownInputKey
		}
		for _, pos := range positions {
			output[pos] = v
		}
		consumedLength += 1
	}

	if consumedLength != len(c.nameToPositions) {
		return nil, ErrUnboundPlaceholder
	}

	return output, nil
}
