package main

import (
	"container/list"
	"fmt"
	"slices"
	"strings"
	"sync"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	toolpolicypb "github.com/02strich/temporal-untrusted-workers/gen/toolpolicy/v1"
)

// The tools of the analytics agent: activities on the "analytics" task queue.
const (
	runQuery       = "analytics/RunQuery"
	publishResults = "analytics/PublishResults"
)

// The policy, in plain Go:
//
//   - RunQuery may read the tables in readable, and never write
//     (queries-are-read-only).
//   - PublishResults is allowed once the run has queried metrics
//     (publish-metrics)...
//   - ...but never once the run has read customers: customer data may be in
//     its results, whatever the report looks like
//     (no-customer-data-in-published-results).
//   - Every other tool is denied.
//
// The rule names match the @ids of the Dogwood version of this policy
// (demos/dogwood-analytics/policies/analytics.dw).
const (
	ruleReadOnly   = "queries-are-read-only"
	rulePublish    = "publish-metrics"
	ruleNoCustomer = "no-customer-data-in-published-results"
)

var readable = map[string]bool{"metrics": true, "customers": true}

const (
	publishedTable = "metrics"
	privateTable   = "customers"
)

// Verifier decides the analytics agent's tool calls. It remembers, per
// workflow run, which tables the run's allowed queries read.
//
// That memory is in this process only: after a restart the verifier has
// forgotten every run, so a run's PublishResults is denied until it queries
// metrics again (fail closed).
type Verifier struct {
	mu   sync.Mutex
	runs *runTables
	// log, if set, receives every evaluation.
	log func(evaluation)
}

// NewVerifier returns a Verifier remembering up to maxRuns runs; the run
// seen least recently is forgotten first.
func NewVerifier(maxRuns int) *Verifier {
	return &Verifier{runs: newRunTables(maxRuns)}
}

// evaluation is what the verifier decided for one tool call, for logging. Its
// JSON shape is the one demos/internal/demokit shows.
type evaluation struct {
	WorkflowID string              `json:"workflowId"`
	Principal  string              `json:"principal"`
	Action     string              `json:"action"`
	Resource   string              `json:"resource"`
	Context    evalContext         `json:"context"`
	History    []map[string]string `json:"history"`
	Decision   string              `json:"decision"`
	Policies   []string            `json:"policies"`
}

// evalContext is what a call was decided on: its parameters, its caller, and
// what its query reads and writes.
type evalContext struct {
	Input  map[string]any `json:"input"`
	Caller map[string]any `json:"caller"`
	SQL    map[string]any `json:"sql,omitempty"`
}

// verdict is the decision on one call: allowed or not, by which rule (none if
// no rule allowed it), and why not.
type verdict struct {
	allowed bool
	rule    string
	reason  string
}

// Decide decides a request's tool calls, in order. The first call not
// allowed denies the request. A RunQuery that is allowed is remembered for
// the run right away, so a later call - even in the same request - sees what
// it read.
func (v *Verifier) Decide(req *toolpolicypb.VerifyToolCallsRequest) *toolpolicypb.VerifyToolCallsResponse {
	caller := req.GetCaller()
	run := caller.GetNamespace() + "/" + caller.GetWorkflowId() + "/" + caller.GetRunId()

	v.mu.Lock()
	defer v.mu.Unlock()
	for _, call := range req.GetToolCalls() {
		ctx := evalContext{Input: inputJSON(call.GetParameters())}
		history := v.runs.history(run)
		var result verdict
		switch call.GetName() {
		case runQuery:
			var access Access
			access, result = decideQuery(call.GetParameters())
			if access.Reads != nil {
				ctx.SQL = map[string]any{"reads": access.ReadList(), "writes": access.WriteList()}
			}
			if result.allowed {
				v.runs.add(run, access.ReadList())
			}
		case publishResults:
			result = decidePublish(v.runs.tables(run))
		default:
			result = verdict{reason: "no rule allows this tool"}
		}
		v.logEvaluation(caller, call.GetName(), ctx, history, result)
		if !result.allowed {
			return &toolpolicypb.VerifyToolCallsResponse{
				Decision: toolpolicypb.Decision_DECISION_DENIED,
				Reason:   denial(call.GetName(), result),
			}
		}
	}
	return &toolpolicypb.VerifyToolCallsResponse{Decision: toolpolicypb.Decision_DECISION_APPROVED}
}

// decideQuery applies queries-are-read-only to a RunQuery call, whose one
// parameter is a JSON object with a "query" string. It also returns what the
// query reads and writes, if it could be analyzed.
func decideQuery(params []*anypb.Any) (Access, verdict) {
	query, ok := queryParam(params)
	if !ok {
		return Access{}, verdict{reason: `RunQuery needs a {"query": "..."} parameter`}
	}
	access, err := Analyze(query)
	if err != nil {
		return Access{}, verdict{reason: "can't analyze the query: " + err.Error()}
	}
	if len(access.Writes) > 0 {
		return access, verdict{reason: "queries are read-only, and this one writes " + strings.Join(access.WriteList(), ", ")}
	}
	for _, table := range access.ReadList() {
		if !readable[table] {
			return access, verdict{reason: fmt.Sprintf("queries may only read %s, and this one reads %s", strings.Join(sortedKeys(readable), " and "), table)}
		}
	}
	return access, verdict{allowed: true, rule: ruleReadOnly}
}

// decidePublish applies no-customer-data-in-published-results and
// publish-metrics to a PublishResults call, given the tables the run read.
func decidePublish(read map[string]bool) verdict {
	switch {
	case read[privateTable]:
		return verdict{
			rule:   ruleNoCustomer,
			reason: "This run read the customers table, so its results may contain customer data and can't be published",
		}
	case read[publishedTable]:
		return verdict{allowed: true, rule: rulePublish}
	default:
		return verdict{reason: "nothing to publish: the run hasn't queried the metrics table"}
	}
}

func denial(tool string, v verdict) string {
	if v.rule != "" {
		return fmt.Sprintf("tool %s denied by rule %s: %s", tool, v.rule, v.reason)
	}
	return fmt.Sprintf("tool %s not permitted: %s", tool, v.reason)
}

// queryParam returns the "query" string of a call's one JSON-object parameter.
func queryParam(params []*anypb.Any) (string, bool) {
	if len(params) != 1 {
		return "", false
	}
	value, ok := jsonValue(params[0])
	if !ok {
		return "", false
	}
	query, ok := value.GetStructValue().GetFields()["query"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return "", false
	}
	return query.StringValue, true
}

// jsonValue returns a parameter that is a plain JSON value (a
// google.protobuf.Value, what the SDK's default converter produces).
func jsonValue(param *anypb.Any) (*structpb.Value, bool) {
	value := &structpb.Value{}
	if !param.MessageIs(value) || param.UnmarshalTo(value) != nil {
		return nil, false
	}
	return value, true
}

// inputJSON renders a call's parameters for logging: plain JSON values as
// themselves, anything else as {}.
func inputJSON(params []*anypb.Any) map[string]any {
	input := map[string]any{"parameterCount": len(params)}
	for i, param := range params {
		arg := any(map[string]any{})
		if value, ok := jsonValue(param); ok {
			arg = value.AsInterface()
		}
		input[fmt.Sprintf("arg%d", i)] = arg
	}
	return input
}

func (v *Verifier) logEvaluation(caller *toolpolicypb.Caller, tool string, ctx evalContext, history []map[string]string, result verdict) {
	if v.log == nil {
		return
	}
	decision := "deny"
	if result.allowed {
		decision = "allow"
	}
	var policies []string
	if result.rule != "" {
		policies = []string{result.rule}
	}
	ctx.Caller = map[string]any{
		"namespace":  caller.GetNamespace(),
		"workflowId": caller.GetWorkflowId(),
		"runId":      caller.GetRunId(),
		"taskQueue":  caller.GetTaskQueue(),
	}
	v.log(evaluation{
		WorkflowID: caller.GetWorkflowId(),
		Principal:  fmt.Sprintf("worker %q", caller.GetSubject()),
		Action:     fmt.Sprintf("tool %q", tool),
		Resource:   fmt.Sprintf("task queue %q", caller.GetTaskQueue()),
		Context:    ctx,
		History:    history,
		Decision:   decision,
		Policies:   policies,
	})
}

// runTables remembers the tables each recent run read, in the order first
// read, forgetting the run seen least recently beyond max.
type runTables struct {
	max   int
	runs  map[string]*list.Element
	order *list.List // of *runEntry, most recently used first
}

type runEntry struct {
	run    string
	tables []string
}

func newRunTables(limit int) *runTables {
	return &runTables{max: max(limit, 1), runs: map[string]*list.Element{}, order: list.New()}
}

func (r *runTables) entry(run string) *runEntry {
	el, ok := r.runs[run]
	if !ok {
		return nil
	}
	r.order.MoveToFront(el)
	return el.Value.(*runEntry)
}

func (r *runTables) add(run string, tables []string) {
	e := r.entry(run)
	if e == nil {
		for r.order.Len() >= r.max {
			oldest := r.order.Back()
			r.order.Remove(oldest)
			delete(r.runs, oldest.Value.(*runEntry).run)
		}
		e = &runEntry{run: run}
		r.runs[run] = r.order.PushFront(e)
	}
	for _, t := range tables {
		if !slices.Contains(e.tables, t) {
			e.tables = append(e.tables, t)
		}
	}
}

func (r *runTables) tables(run string) map[string]bool {
	read := map[string]bool{}
	if e := r.entry(run); e != nil {
		for _, t := range e.tables {
			read[t] = true
		}
	}
	return read
}

// history renders the run's remembered tables for logging, like the Dogwood
// verifier's read_table events.
func (r *runTables) history(run string) []map[string]string {
	var history []map[string]string
	if e := r.entry(run); e != nil {
		for _, t := range e.tables {
			history = append(history, map[string]string{"action": fmt.Sprintf("tool %q", runQuery), "readTable": t})
		}
	}
	return history
}
