// Command cedar-tool-calls drives the Cedar tool-call verification demo (see
// demo.tape; built by up.sh as run/demo).
//
//	demo handlers   - trusted worker, direct to Temporal: Nexus echo/Echo and
//	                  billing/Charge on "nexus-handlers", activity SendEmail
//	                  on "trusted-tools".
//	demo untrusted  - untrusted worker, connected through temporal-proxy, on
//	                  "proxy-test-queue". Its workflow makes the call a case asks for.
//	demo run CASE.. - runs each case and prints whether it completed or was
//	                  blocked, with the proxy's reason (read from PROXY_LOG).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/02strich/temporal-untrusted-workers/demos/internal/demokit"
)

type Charge struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

// cases describes what each demo case does, for display.
var cases = map[string]string{
	"echo":        `Nexus     echo/echo/Echo("hi")`,
	"charge-ok":   `Nexus     payments/billing/Charge({amount: 500, currency: "USD"})`,
	"charge-big":  `Nexus     payments/billing/Charge({amount: 5000, currency: "USD"})`,
	"charge-huge": `Nexus     payments/billing/Charge({amount: 50000, currency: "USD"})`,
	"email-ok":    `Activity  trusted-tools/SendEmail("alice@example.com")`,
	"email-bad":   `Activity  trusted-tools/SendEmail("mallory@evil.com")`,
	"child-cross": `Child     workflow on task queue "other-queue"`,
}

func ToolWorkflow(ctx workflow.Context, kase string) (string, error) {
	nexusOpts := workflow.NexusOperationOptions{ScheduleToCloseTimeout: time.Minute}
	var out string
	switch kase {
	case "echo":
		err := workflow.NewNexusClient("echo", "echo").ExecuteOperation(ctx, "Echo", "hi", nexusOpts).Get(ctx, &out)
		return out, err
	case "charge-ok", "charge-big", "charge-huge":
		amount := map[string]int64{"charge-ok": 500, "charge-big": 5000, "charge-huge": 50000}[kase]
		err := workflow.NewNexusClient("payments", "billing").ExecuteOperation(ctx, "Charge", Charge{Amount: amount, Currency: "USD"}, nexusOpts).Get(ctx, &out)
		return out, err
	case "email-ok", "email-bad":
		to := "alice@example.com"
		if kase == "email-bad" {
			to = "mallory@evil.com"
		}
		actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{TaskQueue: "trusted-tools", StartToCloseTimeout: time.Minute})
		err := workflow.ExecuteActivity(actx, "SendEmail", to).Get(ctx, &out)
		return out, err
	case "child-cross":
		cctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{TaskQueue: "other-queue"})
		err := workflow.ExecuteChildWorkflow(cctx, "Whatever").Get(ctx, &out)
		return out, err
	}
	return "", fmt.Errorf("unknown case %q", kase)
}

func handlers(addr string) {
	c := demokit.Dial(client.Options{HostPort: addr})
	echo := nexus.NewService("echo")
	must(echo.Register(nexus.NewSyncOperation("Echo", func(ctx context.Context, in string, _ nexus.StartOperationOptions) (string, error) {
		return "echo: " + in, nil
	})))
	billing := nexus.NewService("billing")
	must(billing.Register(nexus.NewSyncOperation("Charge", func(ctx context.Context, in Charge, _ nexus.StartOperationOptions) (string, error) {
		return fmt.Sprintf("charged %d %s", in.Amount, in.Currency), nil
	})))
	nw := worker.New(c, "nexus-handlers", worker.Options{})
	nw.RegisterNexusService(echo)
	nw.RegisterNexusService(billing)
	must(nw.Start())
	aw := worker.New(c, "trusted-tools", worker.Options{})
	aw.RegisterActivityWithOptions(func(ctx context.Context, to string) (string, error) {
		return "email sent to " + to, nil
	}, activity.RegisterOptions{Name: "SendEmail"})
	must(aw.Start())
	<-worker.InterruptCh()
	nw.Stop()
	aw.Stop()
}

func untrusted(proxyAddr string) {
	c := demokit.DialProxy(proxyAddr, "testkey123")
	w := worker.New(c, "proxy-test-queue", worker.Options{})
	w.RegisterWorkflow(ToolWorkflow)
	must(w.Run(worker.InterruptCh()))
}

func run(addr, proxyLog string, kases []string) {
	c := demokit.Dial(client.Options{HostPort: addr})
	for _, kase := range kases {
		title, ok := cases[kase]
		if !ok {
			log.Fatalf("unknown case %q", kase)
		}
		demokit.RunCase(c, proxyLog, fmt.Sprintf("demo-%s-%d", kase, time.Now().UnixNano()), demokit.Case{
			Title: title,
			Start: func(ctx context.Context, c client.Client, id string) (client.WorkflowRun, error) {
				return c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: "proxy-test-queue"}, ToolWorkflow, kase)
			},
		})
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	addr := os.Getenv("TEMPORAL_ADDRESS")
	switch os.Args[1] {
	case "handlers":
		handlers(addr)
	case "untrusted":
		untrusted(os.Getenv("PROXY_ADDRESS"))
	case "run":
		run(addr, os.Getenv("PROXY_LOG"), os.Args[2:])
	}
}
