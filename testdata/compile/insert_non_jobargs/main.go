// Misuse: inserting a value that has no Kind method.
package main

import (
	"context"

	"github.com/thnhdatle/foreman"
)

type plain struct{ ID int }

func main() {
	var c *foreman.Client
	_, _ = c.Insert(context.Background(), plain{ID: 1}, nil)
}
