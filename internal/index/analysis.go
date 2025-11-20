package index

import (
	"strings"
	"unicode"
)

// Token represents a single normalized token with its positional offset.
type Token struct {
	Term     string
	Position int
}

// Tokenizer exposes the minimal interface required by the in-memory indexer.
type Tokenizer interface {
	Tokenize(text string) []Token
}

// TokenizerFor returns a tokenizer implementation based on a friendly name.
// Supported options include:
//   - "standard" (default): simple tokenizer without stopword removal
//   - "standard_en": simple tokenizer with a small English stopword list
//
// Any unrecognized name falls back to the standard tokenizer.
func TokenizerFor(name string) Tokenizer {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "standard_en":
		return NewSimpleTokenizer(englishStopwords)
	default:
		return NewSimpleTokenizer(nil)
	}
}

var englishStopwords = []string{
	"a", "an", "the", "and", "or", "but", "of", "to", "in", "on", "for", "with", "as",
	"at", "by", "from", "into", "about", "over", "after", "before", "between", "without",
}

// SimpleTokenizer lowercases input, splits on non-alphanumeric boundaries, and removes stopwords.
type SimpleTokenizer struct {
	stopwords map[string]struct{}
}

// NewSimpleTokenizer constructs a tokenizer that will drop the provided stopwords.
func NewSimpleTokenizer(stopwords []string) *SimpleTokenizer {
	set := make(map[string]struct{}, len(stopwords))
	for _, word := range stopwords {
		set[strings.ToLower(strings.TrimSpace(word))] = struct{}{}
	}
	return &SimpleTokenizer{stopwords: set}
}

// Tokenize splits text into lowercase tokens, skipping any configured stopwords.
func (t *SimpleTokenizer) Tokenize(text string) []Token {
	terms := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})

	tokens := make([]Token, 0, len(terms))
	for idx, term := range terms {
		if term == "" {
			continue
		}
		if _, blocked := t.stopwords[term]; blocked {
			continue
		}
		tokens = append(tokens, Token{Term: term, Position: idx})
	}
	return tokens
}
