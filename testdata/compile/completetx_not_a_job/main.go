// Misuse: CompleteTx needs the *Job passed to Work, not a row.
package main

import (
	"context"

	"github.com/thnhdatle/foreman"
)

func main() {
	_ = foreman.CompleteTx(context.Background(), nil, &foreman.JobRow{})
}
