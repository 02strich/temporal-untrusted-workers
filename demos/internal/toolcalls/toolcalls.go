// Package toolcalls is the harness of the tool-call verification demos
// (cedar-tool-calls, dogwood-tool-calls): the same workers, workflows and
// cases, judged by different verifiers. Each demo's main calls Main, which
// up.sh builds as run/demo:
//
//	demo handlers   - trusted worker, direct to Temporal: Nexus echo/Echo and
//	                  billing/Charge on "nexus-handlers", activities SendEmail
//	                  and IssueRefund on "trusted-tools".
//	demo untrusted  - untrusted worker, connected through temporal-proxy, on
//	                  "proxy-test-queue". Its workflow makes the call a case asks for.
//	demo run CASE.. - runs each case and prints whether it completed or was
//	                  blocked, with the proxy's reason (read from PROXY_LOG) and
//	                  what the verifier evaluated (read from VERIFIER_LOG).
package toolcalls

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
	"email-ok":    `Activity  trusted-tools/SendEmail("alice@example.com")   (the customer's address)`,
	"email-bad":   `Activity  trusted-tools/SendEmail("mallory@evil.com")    (never given to the workflow)`,
	"child-cross": `Child     workflow on task queue "other-queue"`,
	"refund-approved": `Signal    approval({refundId: "R-1001", approvedBy: "manager@example.com"})
  then    Activity  trusted-tools/IssueRefund("R-1001")`,
	"refund-unapproved": `Activity  trusted-tools/IssueRefund("R-1001")   (no approval signal)`,
}

// Input is the workflow's start input. CustomerEmail plays the customer's
// input to an agent: the policy only lets SendEmail reach addresses the
// workflow actually received.
type Input struct {
	Case          string `json:"case"`
	CustomerEmail string `json:"customerEmail"`
	// RefundID is the refund the customer asked for.
	RefundID string `json:"refundId"`
}

// Approval is the payload of the "approval" signal a human sends to approve a
// refund. The policy only allows IssueRefund for a refund an approval signal
// named.
type Approval struct {
	RefundID   string `json:"refundId"`
	ApprovedBy string `json:"approvedBy"`
}

func ToolWorkflow(ctx workflow.Context, in Input) (string, error) {
	kase := in.Case
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
		to := in.CustomerEmail
		if kase == "email-bad" {
			// As if a prompt injection steered the agent elsewhere.
			to = "mallory@evil.com"
		}
		actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{TaskQueue: "trusted-tools", StartToCloseTimeout: time.Minute})
		err := workflow.ExecuteActivity(actx, "SendEmail", to).Get(ctx, &out)
		return out, err
	case "child-cross":
		cctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{TaskQueue: "other-queue"})
		err := workflow.ExecuteChildWorkflow(cctx, "Whatever").Get(ctx, &out)
		return out, err
	case "refund-approved", "refund-unapproved":
		if kase == "refund-approved" {
			// Wait for a human's approval before refunding.
			var approval Approval
			workflow.GetSignalChannel(ctx, "approval").Receive(ctx, &approval)
		}
		// "refund-unapproved" refunds right away, as an agent might without
		// waiting for anyone.
		actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{TaskQueue: "trusted-tools", StartToCloseTimeout: time.Minute})
		err := workflow.ExecuteActivity(actx, "IssueRefund", in.RefundID).Get(ctx, &out)
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
	aw.RegisterActivityWithOptions(func(ctx context.Context, refundID string) (string, error) {
		return "refunded " + refundID, nil
	}, activity.RegisterOptions{Name: "IssueRefund"})
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

func run(addr string, logs demokit.Logs, kases []string) {
	c := demokit.Dial(client.Options{HostPort: addr})
	for _, kase := range kases {
		title, ok := cases[kase]
		if !ok {
			log.Fatalf("unknown case %q", kase)
		}
		demokit.RunCase(c, logs, fmt.Sprintf("demo-%s-%d", kase, time.Now().UnixNano()), demokit.Case{
			Title: title,
			Start: func(ctx context.Context, c client.Client, id string) (client.WorkflowRun, error) {
				in := Input{Case: kase, CustomerEmail: "alice@example.com", RefundID: "R-1001"}
				run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: "proxy-test-queue"}, ToolWorkflow, in)
				if err != nil || kase != "refund-approved" {
					return run, err
				}
				// A manager approves the refund from outside the workflow.
				return run, c.SignalWorkflow(ctx, id, "", "approval", Approval{RefundID: "R-1001", ApprovedBy: "manager@example.com"})
			},
		})
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// Main runs the subcommand in os.Args[1]. logs names the verifier's
// evaluation log target and engine; the log files come from PROXY_LOG and
// VERIFIER_LOG.
func Main(logs demokit.Logs) {
	addr := os.Getenv("TEMPORAL_ADDRESS")
	if len(os.Args) < 2 {
		log.Fatal("usage: demo handlers | untrusted | run CASE...")
	}
	switch os.Args[1] {
	case "handlers":
		handlers(addr)
	case "untrusted":
		untrusted(os.Getenv("PROXY_ADDRESS"))
	case "run":
		logs.Proxy = os.Getenv("PROXY_LOG")
		logs.Verifier = os.Getenv("VERIFIER_LOG")
		run(addr, logs, os.Args[2:])
	default:
		log.Fatalf("unknown command %q", os.Args[1])
	}
}
