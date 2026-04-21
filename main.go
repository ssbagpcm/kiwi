package main

import (
	"fmt"
	"os"

	"kiwi/internal/kiwi"
)

func main() {
	if err := kiwi.Main(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "kiwi:", err)
		os.Exit(1)
	}
}
