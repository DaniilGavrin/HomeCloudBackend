package database

import "testing"

func TestRebindPostgres(t *testing.T) {
	tests := []struct {
		name, input, want string
	}{
		{
			name: "parameters",
			input: "SELECT * FROM items WHERE id=? AND owner=? LIMIT ?",
			want: "SELECT * FROM items WHERE id=$1 AND owner=$2 LIMIT $3",
		},
		{
			name: "quoted strings and identifiers",
			input: "SELECT '?', \"?\", 'it''s ?' FROM \"table?\" WHERE id=?",
			want: "SELECT '?', \"?\", 'it''s ?' FROM \"table?\" WHERE id=$1",
		},
		{
			name: "comments",
			input: "SELECT ? -- ignore ?\n/* outer ? /* nested ? */ still ? */ WHERE id=?",
			want: "SELECT $1 -- ignore ?\n/* outer ? /* nested ? */ still ? */ WHERE id=$2",
		},
		{
			name: "dollar quoted body",
			input: "SELECT $$?$$, $tag$ ? $tag$, ?",
			want: "SELECT $$?$$, $tag$ ? $tag$, $1",
		},
		{
			name: "postgres json operators",
			input: "SELECT data ?| array['a'], data ?& array['b'] FROM items WHERE id=?",
			want: "SELECT data ?| array['a'], data ?& array['b'] FROM items WHERE id=$1",
		},
		{
			name: "postgres json existence operator",
			input: "SELECT data ? 'key' FROM items WHERE id=?",
			want: "SELECT data ? 'key' FROM items WHERE id=$1",
		},
		{
			name: "escaped question mark",
			input: "SELECT ??, ?",
			want: "SELECT ?, $1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rebindPostgres(tt.input); got != tt.want {
				t.Fatalf("rebindPostgres() = %q, want %q", got, tt.want)
			}
		})
	}
}
