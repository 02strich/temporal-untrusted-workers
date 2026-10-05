// Package demokit holds helpers shared by the demo harnesses under demos/:
// Temporal clients without SDK log noise, and a runner that starts a workflow
// and reports whether it completed or temporal-proxy blocked it.
package demokit

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
)

const (
	bold  = "\033[1m"
	dim   = "\033[2m"
	green = "\033[32m"
	red   = "\033[31m"
	reset = "\033[0m"
)

// Dial connects to Temporal with SDK logging silenced, so it doesn't clutter
// demo output. It exits on failure.
func Dial(opts client.Options) client.Client {
	opts.Logger = quietLogger{}
	c, err := client.Dial(opts)
	if err != nil {
		log.Fatal(err)
	}
	return c
}

// DialProxy connects to temporal-proxy at addr (plaintext) with an API key
// from the proxy's auth file, as an untrusted worker would.
func DialProxy(addr, apiKey string) client.Client {
	return Dial(client.Options{
		HostPort:          addr,
		Credentials:       client.NewAPIKeyStaticCredentials(apiKey),
		ConnectionOptions: client.ConnectionOptions{TLSDisabled: true},
	})
}

// Case is one step of a demo: a description to print and a function starting
// the workflow that exercises it.
type Case struct {
	Title string
	Start func(ctx context.Context, c client.Client, workflowID string) (client.WorkflowRun, error)
}

// RunCase starts the case's workflow and prints whether it completed or
// temporal-proxy blocked it. A block is detected from the first "access
// denied" line the proxy logs to proxyLog after the start; the blocked
// workflow is then terminated so its worker stops retrying.
func RunCase(c client.Client, proxyLog, workflowID string, tc Case) {
	fmt.Printf("%s▶ %s%s\n", bold, tc.Title, reset)
	offset := fileSize(proxyLog)
	wf, err := tc.Start(context.Background(), c, workflowID)
	if err != nil {
		log.Fatal(err)
	}

	done := make(chan error, 1)
	var out string
	go func() { done <- wf.Get(context.Background(), &out) }()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(15 * time.Second)
	for {
		select {
		case err := <-done:
			if err != nil {
				fmt.Printf("  %s✖ failed%s   %v\n\n", red, reset, err)
			} else {
				fmt.Printf("  %s✔ allowed%s  %s→ %q%s\n\n", green, reset, dim, out, reset)
			}
			return
		case <-ticker.C:
			if reason := firstDenial(proxyLog, offset); reason != "" {
				fmt.Printf("  %s✖ blocked%s  %s\n\n", red, reset, reason)
				_ = c.TerminateWorkflow(context.Background(), workflowID, "", "demo: blocked by proxy")
				return
			}
			if time.Now().After(deadline) {
				fmt.Printf("  %s? no result yet%s\n\n", red, reset)
				_ = c.TerminateWorkflow(context.Background(), workflowID, "", "demo: timed out")
				return
			}
		}
	}
}

// firstDenial returns the reason of the first "access denied" line the proxy
// logged after offset.
func firstDenial(path string, offset int64) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	if _, err := f.Seek(offset, 0); err != nil {
		return ""
	}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.Contains(line, "access denied") {
			continue
		}
		if i := strings.Index(line, `reason="`); i >= 0 {
			reason := strings.TrimSuffix(line[i+len(`reason="`):], `"`)
			return strings.ReplaceAll(reason, `\"`, `"`)
		}
	}
	return ""
}

func fileSize(path string) int64 {
	st, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return st.Size()
}

type quietLogger struct{}

func (quietLogger) Debug(string, ...interface{}) {}
func (quietLogger) Info(string, ...interface{})  {}
func (quietLogger) Warn(string, ...interface{})  {}
func (quietLogger) Error(string, ...interface{}) {}
