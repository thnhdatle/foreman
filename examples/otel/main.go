// SPDX-License-Identifier: Apache-2.0

// Command otel traces job work with OpenTelemetry.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/examples/internal/exampledb"
)

type ResizeArgs struct {
	Image string `json:"image"`
}

func (ResizeArgs) Kind() string { return "resize" }

func main() {
	if err := run(context.Background()); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	const schema = "example_otel"
	pool, err := exampledb.Open(ctx, schema)
	if err != nil {
		return err
	}
	defer pool.Close()
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = tp.Shutdown(ctx) }()

	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, foreman.WorkFunc[ResizeArgs](func(context.Context, *foreman.Job[ResizeArgs]) error {
		return nil
	}))
	client, err := foreman.NewClient(pool, foreman.Config{
		Schema: schema, Workers: workers, TracerProvider: tp,
		Queues: map[string]foreman.QueueConfig{"default": {}},
	})
	if err != nil {
		return err
	}
	if err := client.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = client.Stop(ctx) }()
	res, err := client.Insert(ctx, ResizeArgs{Image: "cat.png"}, nil)
	if err != nil {
		return err
	}
	if _, err := exampledb.WaitState(ctx, client, res.Job.ID, foreman.StateCompleted, 10*time.Second); err != nil {
		return err
	}
	for _, s := range recorder.Ended() {
		fmt.Printf("span: %s\n", s.Name())
		for _, a := range s.Attributes() {
			fmt.Printf("  %s=%v\n", a.Key, a.Value.AsInterface())
		}
	}
	return nil
}
