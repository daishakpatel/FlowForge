// Command printversion prints the linked version; version_test.go builds it with -ldflags.
package main

import (
	"fmt"

	"github.com/daishakpatel/flowforge/internal/version"
)

func main() {
	fmt.Println(version.Version(), version.Commit())
}
