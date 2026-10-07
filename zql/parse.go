package zql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// ZQL is the text form of a Query. It is deliberately small, because the format
// can only answer a range on one key efficiently:
//
//	SELECT * FROM "binance/spot/BTCUSDT"
//	WHERE datetime >= 1704067200000 AND close > 100.5
//	ORDER BY datetime DESC
//	LIMIT 500
//
// SELECT, ORDER BY and LIMIT are optional. There is no JOIN and no GROUP BY,
// because there is no second key to join against and no aggregate to compute
// without reading every record in the window, which a range scan does anyway.
// Adding them later would mean adding an index, and that is a format change.

// ParseError names where a query stopped being a query. It carries the offset so
// a caller can point at it, because "syntax error" with no position is the least
// useful thing a parser can say.
type ParseError struct {
	Pos int
	Msg string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("zql: at %d: %s", e.Pos, e.Msg)
}

// Parse compiles ZQL text into a Query.
func Parse(src string) (Query, error) {
	toks, err := lex(src)
	if err != nil {
		return Query{}, err
	}

	p := &parser{toks: toks}

	q, err := p.parse()
	if err != nil {
		return Query{}, err
	}

	if p.peek().kind != tokEOF {
		return Query{}, &ParseError{p.peek().pos, fmt.Sprintf("unexpected %q", p.peek().text)}
	}

	return q, nil
}

type tokKind uint8

const (
	tokEOF tokKind = iota
	tokWord
	tokNumber
	tokString
	tokOp
)

type token struct {
	kind tokKind
	text string
	num  float64
	pos  int
}

func lex(src string) ([]token, error) {
	var out []token

	for i := 0; i < len(src); {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '"':
			j := i + 1
			for j < len(src) && src[j] != '"' {
				if src[j] == '\\' && j+1 < len(src) {
					j++
				}

				j++
			}

			if j >= len(src) {
				return nil, &ParseError{i, "unterminated string"}
			}

			out = append(out, token{kind: tokString, text: unescape(src[i+1 : j]), pos: i})
			i = j + 1
		case c == '>' || c == '<' || c == '=' || c == '!':
			if i+1 < len(src) && (src[i+1] == '=' || (c == '<' && src[i+1] == '>')) {
				out = append(out, token{kind: tokOp, text: src[i : i+2], pos: i})
				i += 2

				continue
			}

			if c == '=' {
				out = append(out, token{kind: tokOp, text: "==", pos: i})
				i++

				continue
			}

			out = append(out, token{kind: tokOp, text: string(c), pos: i})
			i++
		case c == '*' || c == ',':
			out = append(out, token{kind: tokOp, text: string(c), pos: i})
			i++
		case c == '-' || c == '.' || unicode.IsDigit(rune(c)):
			j := i
			if src[j] == '-' {
				j++
			}

			for j < len(src) && (unicode.IsDigit(rune(src[j])) || src[j] == '.') {
				j++
			}
			// Exponent notation is refused rather than mis-lexed. Left alone, 1e9
			// would read as the number 1 followed by the word e9, and a filter on
			// the price 1 is a filter that silently returns the whole store.
			if j < len(src) && (src[j] == 'e' || src[j] == 'E') {
				return nil, &ParseError{i, "exponent notation is not supported; write the digits out"}
			}

			v, err := strconv.ParseFloat(src[i:j], 64)
			if err != nil {
				return nil, &ParseError{i, fmt.Sprintf("bad number %q", src[i:j])}
			}

			out = append(out, token{kind: tokNumber, num: v, text: src[i:j], pos: i})
			i = j
		case isWordByte(c):
			j := i
			for j < len(src) && isWordByte(src[j]) {
				j++
			}

			out = append(out, token{kind: tokWord, text: src[i:j], pos: i})
			i = j
		default:
			return nil, &ParseError{i, fmt.Sprintf("unexpected %q", string(c))}
		}
	}

	return append(out, token{kind: tokEOF, pos: len(src)}), nil
}

func isWordByte(c byte) bool {
	return c == '_' || c == '/' || c == '-' || c == '.' || unicode.IsLetter(rune(c)) || unicode.IsDigit(rune(c))
}

func unescape(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}

	var sb strings.Builder

	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
		}

		sb.WriteByte(s[i])
	}

	return sb.String()
}

type parser struct {
	toks []token
	i    int
}

func (p *parser) peek() token { return p.toks[p.i] }

func (p *parser) next() token {
	t := p.toks[p.i]
	if t.kind != tokEOF {
		p.i++
	}

	return t
}

func (p *parser) word(w string) bool {
	if t := p.peek(); t.kind == tokWord && strings.EqualFold(t.text, w) {
		p.i++

		return true
	}

	return false
}

func (p *parser) expectWord(w string) error {
	if p.word(w) {
		return nil
	}

	t := p.peek()

	return &ParseError{t.pos, fmt.Sprintf("expected %s, got %q", w, t.text)}
}

func (p *parser) parse() (Query, error) {
	var q Query

	if p.word("select") {
		m, err := p.parseProjection()
		if err != nil {
			return q, err
		}

		q.Fields = m
	} else if p.peek().kind != tokWord || !strings.EqualFold(p.peek().text, "from") {
		t := p.peek()

		return q, &ParseError{t.pos, fmt.Sprintf("expected SELECT or FROM, got %q", t.text)}
	}

	if err := p.expectWord("from"); err != nil {
		return q, err
	}

	t := p.peek()
	switch t.kind {
	case tokString:
		p.i++
		q.Key = t.text
	case tokWord:
		p.i++
		q.Key = t.text
	default:
		return q, &ParseError{t.pos, "expected a key after FROM"}
	}

	if q.Key == "" {
		return q, &ParseError{t.pos, "empty key"}
	}

	if p.word("range") {
		from, err := p.number()
		if err != nil {
			return q, err
		}

		if err := p.expectWord("to"); err != nil {
			return q, err
		}

		to, err := p.number()
		if err != nil {
			return q, err
		}

		if from > to {
			return q, &ParseError{t.pos, "RANGE starts after it ends"}
		}

		q.From, q.To = from, to
	}

	if p.word("where") {
		if err := p.parseWhere(&q); err != nil {
			return q, err
		}
	}

	if p.word("order") {
		if err := p.expectWord("by"); err != nil {
			return q, err
		}

		t := p.peek()

		f, ok := FieldOf(t.text)
		if !ok || f != FieldDatetime {
			return q, &ParseError{t.pos, "only datetime can be ordered; the format sorts by it"}
		}

		p.i++
		switch {
		case p.word("desc"):
			q.Desc = true
		case p.word("asc"):
			q.Desc = false
		default:
			return q, &ParseError{p.peek().pos, "expected ASC or DESC"}
		}
	}

	if p.word("limit") {
		n, err := p.number()
		if err != nil {
			return q, err
		}

		q.Limit = int(n)
	}

	return q, nil
}

// number reads a non-negative whole token. Times and limits are integers on
// purpose: a fractional millisecond is a mistake, and letting strconv round it
// would turn that mistake into a plausible-looking query.
func (p *parser) number() (uint64, error) {
	t := p.next()
	if t.kind != tokNumber {
		return 0, &ParseError{t.pos, fmt.Sprintf("expected a number, got %q", t.text)}
	}

	if t.num < 0 {
		return 0, &ParseError{t.pos, "expected a non-negative number"}
	}

	if t.num != math.Trunc(t.num) {
		return 0, &ParseError{t.pos, fmt.Sprintf("expected a whole number, got %v", t.num)}
	}

	return uint64(t.num), nil
}

func (p *parser) parseProjection() (FieldMask, error) {
	var m FieldMask

	if t := p.peek(); t.kind == tokOp && t.text == "*" {
		p.i++

		return FAll, nil
	}

	for {
		t := p.next()

		f, ok := FieldOf(t.text)
		if !ok {
			return 0, &ParseError{t.pos, fmt.Sprintf("unknown field %q", t.text)}
		}

		m |= 1 << f

		if t := p.peek(); t.kind == tokOp && t.text == "," {
			p.i++

			continue
		}

		return m, nil
	}
}

func (p *parser) parseWhere(q *Query) error {
	for {
		t := p.next()
		if t.kind != tokWord {
			return &ParseError{t.pos, "expected a field name"}
		}

		f, ok := FieldOf(t.text)
		if !ok {
			return &ParseError{t.pos, fmt.Sprintf("unknown field %q", t.text)}
		}

		o := p.next()
		if o.kind != tokOp {
			return &ParseError{o.pos, "expected a comparison"}
		}

		op, ok := OpOf(o.text)
		if !ok {
			return &ParseError{o.pos, fmt.Sprintf("unknown comparison %q", o.text)}
		}

		v := p.next()
		if v.kind != tokNumber && v.kind != tokWord {
			return &ParseError{v.pos, "expected a number"}
		}

		if v.kind == tokWord {
			fv, err := strconv.ParseFloat(v.text, 64)
			if err != nil {
				return &ParseError{v.pos, fmt.Sprintf("expected a number, got %q", v.text)}
			}

			v.num = fv
		}

		c, err := NewCond(f, op, v.num)
		if err != nil {
			return &ParseError{v.pos, err.Error()}
		}

		q.Conds = append(q.Conds, c)

		if p.word("and") {
			continue
		}

		return nil
	}
}
