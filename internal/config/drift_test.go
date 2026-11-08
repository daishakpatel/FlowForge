package config

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// TestEnvNames_Documented is the documentation drift guard: every variable a service struct
// reads must appear in .env.example (as NAME=, commented or not), in docs/configuration.md and
// in the 00E §6 contract table.
func TestEnvNames_Documented(t *testing.T) {
	t.Parallel()
	root := filepath.Join("..", "..")
	read := func(rel string) string {
		b, err := os.ReadFile(filepath.Join(root, rel)) //nolint:gosec // G304: fixed repo-relative paths
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	envExample := read(".env.example")
	docs := read("docs/configuration.md")
	contract := read("implementation/00E_CONTRACTS_REFERENCE.md")

	seen := map[string]bool{}
	for _, f := range serviceFields() {
		if seen[f.env] {
			continue
		}
		seen[f.env] = true
		name := regexp.QuoteMeta(f.env)
		if !regexp.MustCompile(`(?m)^(# )?` + name + `=`).MatchString(envExample) {
			t.Errorf(".env.example is missing %s", f.env)
		}
		if !regexp.MustCompile("`" + name + "`").MatchString(docs) {
			t.Errorf("docs/configuration.md is missing `%s`", f.env)
		}
		if !regexp.MustCompile(`\b` + name + `\b`).MatchString(contract) {
			t.Errorf("00E §6 is missing %s; add it there first (task 01.03 §5)", f.env)
		}
	}
}
