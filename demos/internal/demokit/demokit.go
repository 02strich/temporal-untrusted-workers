// Package demokit holds helpers shared by the demo harnesses under demos/:
// Temporal clients without SDK log noise, and a runner that starts a workflow
// and reports whether it completed or temporal-proxy blocked it - along with
// the requests the tool verifier (Cedar or Dogwood) evaluated for it.
package demokit

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
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

// Logs are the log files RunCase reads.
type Logs struct {
	// Proxy is temporal-proxy's log; blocks are detected from its
	// "access denied" lines.
	Proxy string
	// Verifier, if set, is the tool verifier's log, written with
	// RUST_LOG=<Target>=debug. RunCase then shows the requests the verifier
	// evaluated for the case: principal, action, resource and context.
	Verifier string
	// Target is the verifier's evaluation log target, e.g. "cedar_request".
	Target string
	// Engine names the verifier's policy engine in the output, e.g. "Cedar".
	Engine string
}

// RunCase starts the case's workflow and prints whether it completed or
// temporal-proxy blocked it, preceded by what the verifier evaluated (if
// logs.Verifier is set). A block is detected from the first "access denied"
// line the proxy logs after the start; the blocked workflow is then
// terminated so its worker stops retrying.
func RunCase(c client.Client, logs Logs, workflowID string, tc Case) {
	fmt.Printf("%s▶ %s%s\n", bold, tc.Title, reset)
	proxyOffset, verifierOffset := fileSize(logs.Proxy), fileSize(logs.Verifier)
	wf, err := tc.Start(context.Background(), c, workflowID)
	if err != nil {
		log.Fatal(err)
	}

	outcome := waitForOutcome(c, wf, logs.Proxy, proxyOffset, workflowID)
	if logs.Verifier != "" {
		printEvaluations(logs.Engine, verifierEvaluations(logs.Verifier, logs.Target, verifierOffset, workflowID))
	}
	fmt.Printf("%s\n\n", outcome)
}

func waitForOutcome(c client.Client, wf client.WorkflowRun, proxyLog string, offset int64, workflowID string) string {
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
				return fmt.Sprintf("  %s✖ failed%s   %v", red, reset, err)
			}
			return fmt.Sprintf("  %s✔ allowed%s  %s→ %q%s", green, reset, dim, out, reset)
		case <-ticker.C:
			if reason := firstDenial(proxyLog, offset); reason != "" {
				_ = c.TerminateWorkflow(context.Background(), workflowID, "", "demo: blocked by proxy")
				return fmt.Sprintf("  %s✖ blocked%s  %s", red, reset, reason)
			}
			if time.Now().After(deadline) {
				_ = c.TerminateWorkflow(context.Background(), workflowID, "", "demo: timed out")
				return fmt.Sprintf("  %s? no result yet%s", red, reset)
			}
		}
	}
}

// evaluation is one evaluation line logged by a tool verifier.
type evaluation struct {
	WorkflowID string          `json:"workflowId"`
	Principal  string          `json:"principal"`
	Action     string          `json:"action"`
	Resource   string          `json:"resource"`
	Context    json.RawMessage `json:"context"`
	// History, logged by the Dogwood verifier, are the latest events the
	// run received, which temporal conditions look back on.
	History  []json.RawMessage `json:"history"`
	Decision string            `json:"decision"`
	// Policies are the @ids of the policies that determined the decision.
	Policies []string `json:"policies"`
}

var ansiEscape = regexp.MustCompile("\x1b\\[[0-9;]*m")

// verifierEvaluations returns the requests the verifier evaluated for
// workflowID after offset, from its log lines of target, one per action: a
// blocked workflow task is retried, but its first evaluation shows what was
// decided.
func verifierEvaluations(path, target string, offset int64, workflowID string) []evaluation {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	if _, err := f.Seek(offset, 0); err != nil {
		return nil
	}
	var evals []evaluation
	seen := map[string]bool{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := ansiEscape.ReplaceAllString(sc.Text(), "")
		i := strings.Index(line, target+":")
		if i < 0 {
			continue
		}
		j := strings.Index(line[i:], "{")
		if j < 0 {
			continue
		}
		var ev evaluation
		if json.Unmarshal([]byte(line[i+j:]), &ev) != nil || ev.WorkflowID != workflowID || seen[ev.Action] {
			continue
		}
		seen[ev.Action] = true
		evals = append(evals, ev)
	}
	return evals
}

func printEvaluations(engine string, evals []evaluation) {
	if len(evals) == 0 {
		fmt.Printf("  %s(%s wasn't asked: nothing reached the verifier)%s\n", dim, engine, reset)
		return
	}
	bar := dim + "│" + reset
	for _, ev := range evals {
		fmt.Printf("  %s┌ %s request%s\n", dim, engine, reset)
		fmt.Printf("  %s %sprincipal%s  %s\n", bar, dim, reset, ev.Principal)
		fmt.Printf("  %s %saction%s     %s\n", bar, dim, reset, ev.Action)
		fmt.Printf("  %s %sresource%s   %s\n", bar, dim, reset, ev.Resource)
		for k, line := range strings.Split(renderJSON(ev.Context, 0, 0), "\n") {
			if k == 0 {
				fmt.Printf("  %s %scontext%s    %s\n", bar, dim, reset, line)
			} else {
				fmt.Printf("  %s            %s\n", bar, line)
			}
		}
		for k, event := range ev.History {
			label := "         "
			if k == 0 {
				label = "history  "
			}
			fmt.Printf("  %s %s%s%s  %s\n", bar, dim, label, reset, compactJSON(event))
		}
		decidedBy := "by " + strings.Join(ev.Policies, ", ")
		if len(ev.Policies) == 0 {
			decidedBy = "(no policy permits it)"
		}
		fmt.Printf("  %s└ decision%s   %s %s%s%s\n", dim, reset, ev.Decision, dim, decidedBy, reset)
	}
}

// renderJSON formats JSON for reading. A value that fits on its line (it
// starts at column col) stays on one line; otherwise an object or array is
// spread over several lines, its members indented two spaces past indent.
// Object fields keep their order.
func renderJSON(raw json.RawMessage, indent, col int) string {
	const maxWidth = 110
	if c := compactJSON(raw); col+len(c) <= maxWidth {
		return c
	}
	pad := strings.Repeat(" ", indent)
	trimmed := bytes.TrimSpace(raw)
	switch {
	case len(trimmed) > 0 && trimmed[0] == '{':
		var b strings.Builder
		b.WriteString("{\n")
		fields := objectFields(trimmed)
		for i, f := range fields {
			key, _ := json.Marshal(f.key)
			prefix := pad + "  " + string(key) + ": "
			b.WriteString(prefix + renderJSON(f.value, indent+2, len(prefix)))
			if i < len(fields)-1 {
				b.WriteString(",")
			}
			b.WriteString("\n")
		}
		b.WriteString(pad + "}")
		return b.String()
	case len(trimmed) > 0 && trimmed[0] == '[':
		var items []json.RawMessage
		_ = json.Unmarshal(trimmed, &items)
		var b strings.Builder
		b.WriteString("[\n")
		for i, item := range items {
			b.WriteString(pad + "  " + renderJSON(item, indent+2, indent+2))
			if i < len(items)-1 {
				b.WriteString(",")
			}
			b.WriteString("\n")
		}
		b.WriteString(pad + "]")
		return b.String()
	}
	return compactJSON(raw)
}

// compactJSON renders JSON on one line, with a space after ':' and ','.
func compactJSON(raw json.RawMessage) string {
	trimmed := bytes.TrimSpace(raw)
	switch {
	case len(trimmed) > 0 && trimmed[0] == '{':
		var parts []string
		for _, f := range objectFields(trimmed) {
			key, _ := json.Marshal(f.key)
			parts = append(parts, string(key)+": "+compactJSON(f.value))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	case len(trimmed) > 0 && trimmed[0] == '[':
		var items []json.RawMessage
		_ = json.Unmarshal(trimmed, &items)
		parts := make([]string, len(items))
		for i, item := range items {
			parts[i] = compactJSON(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	var b bytes.Buffer
	if json.Compact(&b, trimmed) != nil {
		return string(trimmed)
	}
	return b.String()
}

type field struct {
	key   string
	value json.RawMessage
}

// objectFields returns a JSON object's fields in order.
func objectFields(raw []byte) []field {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if _, err := dec.Token(); err != nil {
		return nil
	}
	var fields []field
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return fields
		}
		key, _ := t.(string)
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return fields
		}
		fields = append(fields, field{key, value})
	}
	return fields
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
