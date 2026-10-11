package toolpolicy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	toolpolicypb "github.com/02strich/temporal-untrusted-workers/gen/toolpolicy/v1"
	"github.com/02strich/temporal-untrusted-workers/internal/inputevents"
)

// The default names of the verifier's operations.
const (
	DefaultVerifyOperation = "VerifyToolCalls"
	DefaultRecordOperation = "RecordInputEvents"
)

// NexusVerifier delegates tool-call verification to a Temporal Nexus service,
// invoked as a standalone Nexus operation (StartNexusOperationExecution +
// PollNexusOperationExecution) through Client - normally the proxy's
// upstream connection, so the upstream credentials are reused.
//
// The operation receives a toolpolicypb.VerifyToolCallsRequest as a
// "json/protobuf" payload and must complete (synchronously, or
// asynchronously within Timeout) with a toolpolicypb.VerifyToolCallsResponse
// payload. Only decision=DECISION_APPROVED allows the call: DENIED, ESCALATE
// and unspecified or unknown decisions deny it (ESCALATE with a "pending human
// review" reason), and RETRY_LATER is reported as Unavailable. A failed
// operation, a malformed result, or no result within Timeout makes the proxy
// fail closed with Unavailable. A tool call whose parameters could not be
// rendered (Call.Err) is denied without contacting the verifier.
type NexusVerifier struct {
	Client workflowservice.WorkflowServiceClient
	// Namespace is the caller namespace the standalone operation runs in;
	// the endpoint must allow it.
	Namespace string
	Endpoint  string
	Service   string
	// VerifyOperation decides tool calls (VerifyToolCalls); see Verify.
	VerifyOperation string
	// RecordOperation receives input events (RecordInputEvents); see
	// RecordInputEvents.
	RecordOperation string
	Timeout         time.Duration
}

func (v *NexusVerifier) Verify(ctx context.Context, req Request) error {
	for _, call := range req.Calls {
		if call.Err != nil {
			return DeniedError{Reason: call.Err.Error()}
		}
	}

	input, err := encodeVerifyRequest(req)
	if err != nil {
		return UnavailableError{Err: err}
	}
	result, err := v.run(ctx, v.VerifyOperation, input)
	if err != nil {
		return err
	}
	return decodeVerifyResponse(result)
}

// RecordInputEvents sends a batch of input events for the caller's run to the
// verifier's RecordOperation and waits for its acknowledgement. The verifier
// doesn't judge the events; it keeps them for deciding the run's later tool
// calls. Any successful result is an acknowledgement; failures are returned as
// UnavailableError.
func (v *NexusVerifier) RecordInputEvents(ctx context.Context, caller Caller, batch inputevents.Batch) error {
	input, err := encodeRecordInputEventsRequest(caller, batch)
	if err != nil {
		return UnavailableError{Err: err}
	}
	_, err = v.run(ctx, v.RecordOperation, input)
	return err
}

// run executes operation as a standalone Nexus operation with input and
// returns its result once it completes, within Timeout. Failures are returned
// as UnavailableError.
func (v *NexusVerifier) run(ctx context.Context, operation string, input *commonpb.Payload) (*commonpb.Payload, error) {
	ctx, cancel := context.WithTimeout(ctx, v.Timeout)
	defer cancel()

	operationID, err := randomID()
	if err != nil {
		return nil, UnavailableError{Err: err}
	}
	requestID, err := randomID()
	if err != nil {
		return nil, UnavailableError{Err: err}
	}

	startResp, err := v.Client.StartNexusOperationExecution(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		Namespace:              v.Namespace,
		Identity:               "temporal-proxy",
		RequestId:              requestID,
		OperationId:            operationID,
		Endpoint:               v.Endpoint,
		Service:                v.Service,
		Operation:              operation,
		ScheduleToCloseTimeout: durationpb.New(v.Timeout),
		Input:                  input,
	})
	if err != nil {
		return nil, UnavailableError{Err: fmt.Errorf("starting nexus operation %s: %w", operation, err)}
	}

	for {
		pollResp, err := v.Client.PollNexusOperationExecution(ctx, &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   v.Namespace,
			OperationId: operationID,
			RunId:       startResp.GetRunId(),
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
		})
		if err != nil {
			return nil, UnavailableError{Err: fmt.Errorf("polling nexus operation %s: %w", operation, err)}
		}
		if pollResp.GetWaitStage() != enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED {
			// The long poll returned before the operation closed; poll again
			// until it does or ctx expires.
			if err := ctx.Err(); err != nil {
				return nil, UnavailableError{Err: err}
			}
			continue
		}
		if f := pollResp.GetFailure(); f != nil {
			return nil, UnavailableError{Err: fmt.Errorf("nexus operation %s failed: %s", operation, f.GetMessage())}
		}
		return pollResp.GetResult(), nil
	}
}

// The request is built as JSON directly rather than through
// toolpolicypb.VerifyToolCallsRequest, because the parameters are protobuf
// JSON Anys of types the proxy usually does not link in (see payloadParameter).
// The field names mirror the protobuf JSON mapping of toolpolicy.proto.
type wireRequest struct {
	Caller    wireCaller     `json:"caller"`
	ToolCalls []wireToolCall `json:"toolCalls,omitempty"`
}

type wireCaller struct {
	Namespace  string `json:"namespace,omitempty"`
	Subject    string `json:"subject,omitempty"`
	TaskQueue  string `json:"taskQueue,omitempty"`
	WorkflowID string `json:"workflowId,omitempty"`
	RunID      string `json:"runId,omitempty"`
}

func newWireCaller(c Caller) wireCaller {
	return wireCaller{
		Namespace:  c.Identity.Namespace,
		Subject:    c.Identity.Subject,
		TaskQueue:  c.TaskQueue,
		WorkflowID: c.WorkflowID,
		RunID:      c.RunID,
	}
}

// wireRecordInputEventsRequest mirrors RecordInputEventsRequest's protobuf
// JSON mapping (see wireRequest for why it is built by hand).
type wireRecordInputEventsRequest struct {
	Caller           wireCaller       `json:"caller"`
	Events           []wireInputEvent `json:"events,omitempty"`
	FromHistoryStart bool             `json:"fromHistoryStart,omitempty"`
}

type wireInputEvent struct {
	Kind     string            `json:"kind"`
	Name     string            `json:"name,omitempty"`
	Payloads []json.RawMessage `json:"payloads,omitempty"`
	// EventID is an int64, which the protobuf JSON mapping renders as a string.
	EventID  string `json:"eventId,omitempty"`
	UpdateID string `json:"updateId,omitempty"`
}

func encodeRecordInputEventsRequest(caller Caller, batch inputevents.Batch) (*commonpb.Payload, error) {
	wire := wireRecordInputEventsRequest{Caller: newWireCaller(caller), FromHistoryStart: batch.FromHistoryStart}
	for _, ev := range batch.Events {
		we := wireInputEvent{Kind: ev.Kind.ProtoName(), Name: ev.Name, UpdateID: ev.UpdateID}
		if ev.EventID != 0 {
			we.EventID = strconv.FormatInt(ev.EventID, 10)
		}
		for _, p := range ev.Payloads {
			param, err := payloadParameter(p)
			if err != nil {
				// Input events are context, never judged: pass what the proxy
				// can't render as the opaque payload rather than failing.
				if param, err = marshalAny(p); err != nil {
					return nil, fmt.Errorf("encoding input event payload: %w", err)
				}
			}
			we.Payloads = append(we.Payloads, param)
		}
		wire.Events = append(wire.Events, we)
	}
	return jsonProtoPayload(wire, &toolpolicypb.RecordInputEventsRequest{})
}

// jsonProtoPayload encodes v as a "json/protobuf" payload of msg's type.
func jsonProtoPayload(v any, msg proto.Message) (*commonpb.Payload, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encoding verifier request: %w", err)
	}
	return &commonpb.Payload{
		Metadata: map[string][]byte{
			metadataEncoding:    []byte(encodingJSONProto),
			metadataMessageType: []byte(msg.ProtoReflect().Descriptor().FullName()),
		},
		Data: data,
	}, nil
}

type wireToolCall struct {
	Name       string            `json:"name,omitempty"`
	Parameters []json.RawMessage `json:"parameters,omitempty"`
}

func encodeVerifyRequest(req Request) (*commonpb.Payload, error) {
	wire := wireRequest{Caller: newWireCaller(req.Caller)}
	for _, call := range req.Calls {
		wire.ToolCalls = append(wire.ToolCalls, wireToolCall{Name: call.Name, Parameters: call.Parameters})
	}
	return jsonProtoPayload(wire, &toolpolicypb.VerifyToolCallsRequest{})
}

func decodeVerifyResponse(result *commonpb.Payload) error {
	if result == nil {
		return UnavailableError{Err: errors.New("nexus operation returned no result")}
	}
	var out toolpolicypb.VerifyToolCallsResponse
	var err error
	if string(result.GetMetadata()[metadataEncoding]) == encodingBinaryProto {
		err = proto.Unmarshal(result.GetData(), &out)
	} else {
		// "json/protobuf", or plain JSON in the protobuf JSON mapping from a
		// handler that doesn't use the generated types. The encoding is not
		// checked further: depending on how the handler's SDK and the server
		// translate the Nexus content type, the metadata may differ.
		err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(result.GetData(), &out)
	}
	if err != nil {
		return UnavailableError{Err: fmt.Errorf("decoding verifier result: %w", err)}
	}
	return decisionError(out.GetDecision(), out.GetReason())
}

// decisionError maps a verifier decision to the Verify result. Only
// DECISION_APPROVED allows the call; an unspecified or unknown decision is a
// denial (fail closed).
func decisionError(decision toolpolicypb.Decision, reason string) error {
	switch decision {
	case toolpolicypb.Decision_DECISION_APPROVED:
		return nil
	case toolpolicypb.Decision_DECISION_ESCALATE:
		if reason == "" {
			reason = "escalated by tool verifier"
		}
		return DeniedError{Reason: "pending human review: " + reason}
	case toolpolicypb.Decision_DECISION_RETRY_LATER:
		if reason == "" {
			reason = "no reason given"
		}
		return UnavailableError{Err: fmt.Errorf("verifier asked to retry later: %s", reason)}
	case toolpolicypb.Decision_DECISION_DENIED:
		if reason == "" {
			reason = "tool calls rejected by tool verifier"
		}
		return DeniedError{Reason: reason}
	default:
		if reason == "" {
			reason = fmt.Sprintf("tool verifier returned no usable decision (%v)", decision)
		}
		return DeniedError{Reason: reason}
	}
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generating operation id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
