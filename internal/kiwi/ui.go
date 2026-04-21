package kiwi

import (
	"fmt"
	"time"
)

const bannerText = `
,--. ,--.,--.,--.   ,--.,--.
|  .'   /|  ||  |   |  ||  |
|  .   ' |  ||  |.'.|  ||  |
|  |\   \|  ||   ,'.   ||  |
` + "`" + `--' '--'` + "`" + `--''--'   '--'` + "`" + `--'
`

func banner() string {
	return bannerText
}

func withSpinner[T any](label string, fn func() (T, error)) (T, error) {
	frames := []string{"|", "/", "-", "\\"}
	done := make(chan struct{})
	go func() {
		index := 0
		ticker := time.NewTicker(90 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				fmt.Printf("\r[%s] %s", frames[index%len(frames)], label)
				index++
			}
		}
	}()
	value, err := fn()
	close(done)
	if err != nil {
		fmt.Printf("\r[!!] %s\n", label)
		return value, err
	}
	fmt.Printf("\r[ok] %s\n", label)
	return value, nil
}

func withSpinnerNoValue(label string, fn func() error) error {
	_, err := withSpinner(label, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}

func printBlock(title string, lines ...string) {
	if title != "" {
		fmt.Println(title)
	}
	for _, line := range lines {
		fmt.Println(line)
	}
}
