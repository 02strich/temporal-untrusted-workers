package demokit

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRenderJSON(t *testing.T) {
	short := json.RawMessage(`{"b":1,"a":["x","y"]}`)
	if got, want := renderJSON(short, 0, 0), `{"b": 1, "a": ["x", "y"]}`; got != want {
		t.Fatalf("short value: got %q, want %q (one line, field order kept)", got, want)
	}

	long := json.RawMessage(`{"workflowId":"demo-email-ok-1760000000000000000","parameters":{"arg0":{"type":"google.protobuf.Value","data":{"value":"alice@example.com"}}},"inputEventsComplete":true}`)
	want := `{
  "workflowId": "demo-email-ok-1760000000000000000",
  "parameters": {"arg0": {"type": "google.protobuf.Value", "data": {"value": "alice@example.com"}}},
  "inputEventsComplete": true
}`
	if got := renderJSON(long, 0, 0); got != want {
		t.Fatalf("long value:\ngot\n%s\nwant\n%s", got, want)
	}
}

func TestRenderJSON_NestedIndent(t *testing.T) {
	raw := json.RawMessage(`{"parameters":{"arg0":{"type":"google.protobuf.Value","data":{"amount":5000,"currency":"USD","note":"a fairly long note to push this past the width"}}}}`)
	want := `{
  "parameters": {
    "arg0": {
      "type": "google.protobuf.Value",
      "data": {"amount": 5000, "currency": "USD", "note": "a fairly long note to push this past the width"}
    }
  }
}`
	if got := renderJSON(raw, 0, 0); got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
}

func TestVerifierEvaluations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "verifier.log")
	lines := "" +
		"2026-10-11T08:00:00Z  INFO cedar_verifier: cedar-verifier running\n" +
		"\x1b[2m2026-10-11T08:00:01Z\x1b[0m DEBUG \x1b[2mcedar_request\x1b[0m\x1b[2m:\x1b[0m " +
		`{"workflowId":"wf-1","principal":"Worker::\"fleet-a\"","action":"Action::\"a/B\"","resource":"TaskQueue::\"q\"","context":{"x":1},"decision":"deny"}` + "\n" +
		// The retried workflow task's evaluation of the same action is skipped.
		"2026-10-11T08:00:02Z DEBUG cedar_request: " +
		`{"workflowId":"wf-1","principal":"Worker::\"fleet-a\"","action":"Action::\"a/B\"","resource":"TaskQueue::\"q\"","context":{"x":1},"decision":"deny"}` + "\n" +
		"2026-10-11T08:00:03Z DEBUG cedar_request: " +
		`{"workflowId":"wf-2","principal":"p","action":"Action::\"c/D\"","resource":"r","context":{},"decision":"allow"}` + "\n"
	if err := os.WriteFile(path, []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}

	evals := verifierEvaluations(path, "cedar_request", 0, "wf-1")
	if len(evals) != 1 {
		t.Fatalf("expected one evaluation for wf-1, got %+v", evals)
	}
	if ev := evals[0]; ev.Principal != `Worker::"fleet-a"` || ev.Action != `Action::"a/B"` || ev.Decision != "deny" || string(ev.Context) != `{"x":1}` {
		t.Fatalf("unexpected evaluation %+v", ev)
	}
}

func TestVerifierEvaluations_DogwoodHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "verifier.log")
	lines := "" +
		// A Cedar line in the same file is ignored for the Dogwood target.
		`2026-10-11T08:00:00Z DEBUG cedar_request: {"workflowId":"wf-1","action":"x","decision":"deny"}` + "\n" +
		`2026-10-11T08:00:01Z DEBUG dogwood_request: {"workflowId":"wf-1","principal":"Temporal::Worker::\"fleet-a\"",` +
		`"action":"Temporal::Action::\"trusted-tools/IssueRefund\"","resource":"Temporal::TaskQueue::\"q\"","context":{"input":{"arg0":"R-1001"}},` +
		`"history":[{"action":"Temporal::Action::\"signal/approval\"","input":{"arg0":{"refundId":"R-1001"}}}],` +
		`"decision":"allow","policies":["refund-needs-approval"]}` + "\n"
	if err := os.WriteFile(path, []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}

	evals := verifierEvaluations(path, "dogwood_request", 0, "wf-1")
	if len(evals) != 1 {
		t.Fatalf("expected one Dogwood evaluation for wf-1, got %+v", evals)
	}
	ev := evals[0]
	if len(ev.History) != 1 || compactJSON(ev.History[0]) != `{"action": "Temporal::Action::\"signal/approval\"", "input": {"arg0": {"refundId": "R-1001"}}}` {
		t.Fatalf("unexpected history %q", ev.History)
	}
	if len(ev.Policies) != 1 || ev.Policies[0] != "refund-needs-approval" {
		t.Fatalf("unexpected policies %v", ev.Policies)
	}
}
