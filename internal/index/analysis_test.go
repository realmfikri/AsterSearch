package index

import "testing"

func TestTokenizerForStandardEn(t *testing.T) {
	tokenizer := TokenizerFor("standard_en")
	tokens := tokenizer.Tokenize("The quick brown fox and the dog")
	if len(tokens) != 4 { // stopwords should be removed
		t.Fatalf("expected 4 tokens after stopword removal, got %d", len(tokens))
	}
	if tokens[0].Term != "quick" || tokens[len(tokens)-1].Term != "dog" {
		t.Fatalf("unexpected token ordering: %+v", tokens)
	}
}

func TestTokenizerForDefault(t *testing.T) {
	tokenizer := TokenizerFor("custom")
	tokens := tokenizer.Tokenize("The quick brown fox")
	if len(tokens) != 4 { // no stopwords filtered
		t.Fatalf("expected 4 tokens with default tokenizer, got %d", len(tokens))
	}
	if tokens[0].Term != "the" {
		t.Fatalf("expected stopwords to remain for default tokenizer")
	}
}
