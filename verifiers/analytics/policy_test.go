package main

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	toolpolicypb "github.com/02strich/temporal-untrusted-workers/gen/toolpolicy/v1"
)

func param(t *testing.T, v any) *anypb.Any {
	t.Helper()
	value, err := structpb.NewValue(v)
	if err != nil {
		t.Fatal(err)
	}
	a, err := anypb.New(value)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func query(t *testing.T, sql string) *toolpolicypb.ToolCall {
	return &toolpolicypb.ToolCall{Name: runQuery, Parameters: []*anypb.Any{param(t, map[string]any{"query": sql})}}
}

func publish(t *testing.T) *toolpolicypb.ToolCall {
	return &toolpolicypb.ToolCall{Name: publishResults, Parameters: []*anypb.Any{
		param(t, map[string]any{"channel": "#kpi-weekly", "title": "KPIs", "body": "up"}),
	}}
}

func request(run string, calls ...*toolpolicypb.ToolCall) *toolpolicypb.VerifyToolCallsRequest {
	return &toolpolicypb.VerifyToolCallsRequest{
		Caller:    &toolpolicypb.Caller{Namespace: "default", Subject: "fleet-a", TaskQueue: "proxy-test-queue", WorkflowId: "insights", RunId: run},
		ToolCalls: calls,
	}
}

func assertAllowed(t *testing.T, v *Verifier, req *toolpolicypb.VerifyToolCallsRequest) {
	t.Helper()
	if resp := v.Decide(req); resp.GetDecision() != toolpolicypb.Decision_DECISION_APPROVED {
		t.Errorf("want approved, got %v: %s", resp.GetDecision(), resp.GetReason())
	}
}

func assertDenied(t *testing.T, v *Verifier, req *toolpolicypb.VerifyToolCallsRequest, reason string) {
	t.Helper()
	resp := v.Decide(req)
	if resp.GetDecision() != toolpolicypb.Decision_DECISION_DENIED || !strings.Contains(resp.GetReason(), reason) {
		t.Errorf("want denied with %q, got %v: %s", reason, resp.GetDecision(), resp.GetReason())
	}
}

const (
	metricsQuery = "SELECT week, active_users, signups, churn_pct FROM metrics WHERE week >= '2026-10-05' ORDER BY week"
	joinedQuery  = "SELECT c.company, c.plan, sum(m.signups) AS signups FROM metrics m JOIN customers c ON c.id = m.customer_id " +
		"WHERE m.week = '2026-10-19' GROUP BY c.company, c.plan ORDER BY signups DESC LIMIT 3"
)

// The dogwood-analytics demo's cases, each in a run of its own.
func TestDemoCases(t *testing.T) {
	v := NewVerifier(100)

	// weekly-kpis
	assertAllowed(t, v, request("weekly-kpis", query(t, metricsQuery)))
	assertAllowed(t, v, request("weekly-kpis", publish(t)))

	// top-accounts
	assertAllowed(t, v, request("top-accounts", query(t, joinedQuery)))
	assertDenied(t, v, request("top-accounts", publish(t)),
		"tool analytics/PublishResults denied by rule no-customer-data-in-published-results: This run read the customers table")

	// injected
	assertAllowed(t, v, request("injected", query(t, "SELECT email FROM customers WHERE created_at >= '2026-10-01'")))
	assertAllowed(t, v, request("injected", query(t, metricsQuery)))
	assertDenied(t, v, request("injected", publish(t)), ruleNoCustomer)

	// fix-the-numbers
	assertDenied(t, v, request("fix-the-numbers", query(t, "UPDATE metrics SET churn_pct = 2.1 WHERE week = '2026-10-26'")),
		"tool analytics/RunQuery not permitted: queries are read-only, and this one writes metrics")

	// drop-table
	assertDenied(t, v, request("drop-table", query(t, "DROP TABLE customers")), "can't analyze the query: unsupported SQL statement")

	// publish-only
	assertDenied(t, v, request("publish-only", publish(t)), "nothing to publish")
}

func TestQueries(t *testing.T) {
	v := NewVerifier(100)
	assertDenied(t, v, request("r", query(t, "SELECT * FROM invoices")), "this one reads invoices")
	assertDenied(t, v, request("r", query(t, "SELECT * FROM metrics, public.customers")), "reads public.customers")
	assertDenied(t, v, request("r", query(t, "SELEC 1")), "can't analyze")
	// The parameter must be {"query": "..."}.
	assertDenied(t, v, request("r", &toolpolicypb.ToolCall{Name: runQuery}), "needs a")
	assertDenied(t, v, request("r", &toolpolicypb.ToolCall{Name: runQuery, Parameters: []*anypb.Any{param(t, "SELECT 1")}}), "needs a")
	assertDenied(t, v, request("r", &toolpolicypb.ToolCall{Name: runQuery, Parameters: []*anypb.Any{param(t, map[string]any{"query": 5})}}), "needs a")
	// Other tools are denied.
	assertDenied(t, v, request("r", &toolpolicypb.ToolCall{Name: "analytics/DeleteEverything"}), "no rule allows this tool")
	// Denied queries aren't remembered: nothing to publish.
	assertDenied(t, v, request("r", publish(t)), "nothing to publish")
}

func TestRunsAreSeparate(t *testing.T) {
	v := NewVerifier(100)
	assertAllowed(t, v, request("a", query(t, metricsQuery)))
	assertAllowed(t, v, request("b", query(t, joinedQuery)))
	assertAllowed(t, v, request("a", publish(t)))
	assertDenied(t, v, request("b", publish(t)), ruleNoCustomer)
	assertDenied(t, v, request("c", publish(t)), "nothing to publish")
}

func TestSameRequest(t *testing.T) {
	v := NewVerifier(100)
	// A query and a publish in one workflow task: the publish sees the query.
	assertAllowed(t, v, request("a", query(t, metricsQuery), publish(t)))
	assertDenied(t, v, request("b", query(t, metricsQuery), query(t, "SELECT email FROM customers"), publish(t)), ruleNoCustomer)
	// A denied call denies the request, even after allowed ones.
	assertDenied(t, v, request("c", query(t, metricsQuery), query(t, "DELETE FROM metrics")), "read-only")
}

func TestForgottenRunFailsClosed(t *testing.T) {
	v := NewVerifier(2)
	assertAllowed(t, v, request("a", query(t, metricsQuery)))
	assertAllowed(t, v, request("b", query(t, metricsQuery)))
	assertAllowed(t, v, request("c", query(t, metricsQuery)))
	// "a" was seen least recently and is forgotten: publishing fails closed.
	assertDenied(t, v, request("a", publish(t)), "nothing to publish")
	assertAllowed(t, v, request("c", publish(t)))
}

func TestEvaluationLog(t *testing.T) {
	v := NewVerifier(100)
	var logged []evaluation
	v.log = func(e evaluation) { logged = append(logged, e) }
	v.Decide(request("a", query(t, joinedQuery)))
	v.Decide(request("a", publish(t)))
	if len(logged) != 2 {
		t.Fatalf("logged %d evaluations, want 2", len(logged))
	}
	q, p := logged[0], logged[1]
	sql := q.Context.SQL
	if got := strings.Join(sql["reads"].([]string), ","); got != "customers,metrics" {
		t.Errorf("logged reads %q", got)
	}
	if q.Decision != "allow" || q.Policies[0] != ruleReadOnly || q.WorkflowID != "insights" {
		t.Errorf("query evaluation %+v", q)
	}
	if p.Decision != "deny" || p.Policies[0] != ruleNoCustomer || len(p.History) != 2 || p.History[0]["readTable"] != "customers" {
		t.Errorf("publish evaluation %+v", p)
	}
}
