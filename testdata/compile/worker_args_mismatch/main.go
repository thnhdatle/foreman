// Misuse: registering a worker for SMSArgs as the worker of EmailArgs.
package main

import (
	"context"

	"github.com/thnhdatle/foreman"
)

type EmailArgs struct{}

func (EmailArgs) Kind() string { return "email" }

type SMSArgs struct{}

func (SMSArgs) Kind() string { return "sms" }

type SMSWorker struct{}

func (SMSWorker) Work(context.Context, *foreman.Job[SMSArgs]) error { return nil }

func main() {
	foreman.AddWorker[EmailArgs](foreman.NewWorkers(), SMSWorker{})
}
