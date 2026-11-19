// Command flowforgectl is the FlowForge command-line client (task 03.09): it drives the REST API
// at FF_API_URL with the API key in FF_API_KEY. See docs/api/cli.md.
//
//	flowforgectl [-o table|json] <resource> <verb> [args] [flags]
//
// Exit codes: 0 success, 1 an API or network error (the API's error envelope is printed to
// stderr), 2 a usage error.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
)

const defaultAPIURL = "http://localhost:8080"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// env is what every command gets.
type env struct {
	c      *client
	out    printer
	stdout io.Writer
	stderr io.Writer
}

type command func(ctx context.Context, e *env, args []string) error

// usageError is a mistake in the command line: exit code 2.
type usageError struct{ msg string }

func (u usageError) Error() string { return u.msg }

func usagef(format string, a ...any) error { return usageError{fmt.Sprintf(format, a...)} }

// run executes one command line and returns the exit code.
func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	args, format, err := outputFormat(args)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "flowforgectl:", err)
		return 2
	}
	if len(args) < 2 {
		_, _ = fmt.Fprint(stderr, usage())
		return 2
	}
	cmd, ok := commands()[args[0]+" "+args[1]]
	if !ok {
		_, _ = fmt.Fprintf(stderr, "flowforgectl: unknown command %q\n%s", strings.Join(args[:2], " "), usage())
		return 2
	}
	base := getenv("FF_API_URL")
	if base == "" {
		base = defaultAPIURL
	}
	e := &env{c: newClient(base, getenv("FF_API_KEY")), out: printer{w: stdout, json: format == "json"}, stdout: stdout, stderr: stderr}
	err = cmd(ctx, e, args[2:])
	var ue usageError
	var ae *apiError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &ue):
		_, _ = fmt.Fprintln(stderr, "flowforgectl:", err)
		return 2
	case errors.As(err, &ae):
		_, _ = fmt.Fprintln(stderr, "flowforgectl: API error:", ae)
		_ = printer{w: stderr, json: true}.print(map[string]any{"error": ae})
		return 1
	}
	_, _ = fmt.Fprintln(stderr, "flowforgectl:", err)
	return 1
}

// outputFormat removes -o/--output (anywhere in args) and returns the format: table or json.
func outputFormat(args []string) ([]string, string, error) {
	format, rest := "table", make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "-o" || a == "--output":
			if i+1 == len(args) {
				return nil, "", errors.New(a + " needs a value: table or json")
			}
			format = args[i+1]
			i++
		case strings.HasPrefix(a, "-o=") || strings.HasPrefix(a, "--output="):
			format = a[strings.Index(a, "=")+1:]
		default:
			rest = append(rest, a)
		}
	}
	if format != "table" && format != "json" {
		return nil, "", fmt.Errorf("output must be table or json, not %q", format)
	}
	return rest, format, nil
}

// parse parses fs's flags anywhere among args and returns the positional arguments, checking
// there are exactly want of them (want < 0: any number).
func parse(fs *flag.FlagSet, args []string, want int, usage string) ([]string, error) {
	fs.SetOutput(io.Discard)
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, usagef("%v (usage: %s)", err, usage)
		}
		if fs.NArg() == 0 {
			break
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
	if want >= 0 && len(pos) != want {
		return nil, usagef("usage: %s", usage)
	}
	return pos, nil
}

func usage() string {
	names := make([]string, 0, len(commands()))
	for n := range commands() {
		names = append(names, n)
	}
	sort.Strings(names)
	return "usage: flowforgectl [-o table|json] <command> [args]\ncommands:\n  " + strings.Join(names, "\n  ") +
		"\nenvironment: FF_API_URL (default " + defaultAPIURL + "), FF_API_KEY\n"
}
