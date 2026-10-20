// Misuse: decoding args into a type that is not JobArgs.
package main

import "github.com/thnhdatle/foreman"

func main() {
	_, _ = foreman.DecodeArgs[map[string]any](&foreman.JobRow{})
}
