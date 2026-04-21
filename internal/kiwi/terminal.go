package kiwi

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"unicode"

	"golang.org/x/term"
)

type terminalReadWriter struct {
	in  *os.File
	out *os.File
}

func (rw terminalReadWriter) Read(p []byte) (int, error) {
	return rw.in.Read(p)
}

func (rw terminalReadWriter) Write(p []byte) (int, error) {
	return rw.out.Write(p)
}

func RunTerminal() error {
	if !term.IsTerminal(int(os.Stdin.Fd())) || !term.IsTerminal(int(os.Stdout.Fd())) {
		return fmt.Errorf("terminal requires a tty")
	}
	if os.Geteuid() != 0 {
		return rerunTerminalWithSudo()
	}
	reader := term.NewTerminal(terminalReadWriter{in: os.Stdin, out: os.Stdout}, "(kiwi) ")
	if width, height, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
		_ = reader.SetSize(width, height)
	}
	reader.SetBracketedPasteMode(true)
	for {
		state, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err != nil {
			return err
		}
		line, readErr := reader.ReadLine()
		_ = term.Restore(int(os.Stdin.Fd()), state)
		if readErr != nil {
			if readErr == io.EOF {
				fmt.Println()
				return nil
			}
			return readErr
		}
		fields, err := parseCommandLine(line)
		if err != nil {
			fmt.Fprintln(os.Stderr, "kiwi:", err)
			continue
		}
		if len(fields) == 0 {
			continue
		}
		switch strings.ToLower(fields[0]) {
		case "exit", "quit":
			return nil
		case "clear":
			_, _ = os.Stdout.Write([]byte("\x1b[2J\x1b[H"))
			continue
		}
		if err := Main(fields); err != nil {
			fmt.Fprintln(os.Stderr, "kiwi:", err)
		}
	}
}

func rerunTerminalWithSudo() error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	if _, err := exec.LookPath("sudo"); err != nil {
		return fmt.Errorf("terminal needs sudo; run: sudo %s terminal", executable)
	}
	cmd := exec.Command("sudo", "-E", executable, "terminal")
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func parseCommandLine(line string) ([]string, error) {
	fields := []string{}
	var current strings.Builder
	quote := rune(0)
	escaped := false
	for _, r := range line {
		if escaped {
			current.WriteRune(r)
			escaped = false
			continue
		}
		switch {
		case r == '\\' && quote != '\'':
			escaped = true
		case quote != 0:
			if r == quote {
				quote = 0
				continue
			}
			current.WriteRune(r)
		case r == '\'' || r == '"':
			quote = r
		case unicode.IsSpace(r):
			if current.Len() == 0 {
				continue
			}
			fields = append(fields, current.String())
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}
	if escaped {
		return nil, fmt.Errorf("unfinished escape")
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote")
	}
	if current.Len() > 0 {
		fields = append(fields, current.String())
	}
	return fields, nil
}
