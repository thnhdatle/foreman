// Misuse: a job whose args type is not JobArgs.
package main

import "github.com/thnhdatle/foreman"

func main() {
	var j foreman.Job[string]
	_ = j
}
