package toolpolicy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	toolpolicypb "github.com/02strich/temporal-untrusted-workers/gen/toolpolicy/v1"
)

// DefaultNexusOperation is the operation name NexusVerifier calls when none
// is configured.
const DefaultNexusOperation = "VerifyToolCalls"

// NexusVerifier delegates tool-call verification to a Temporal Nexus service,
// invoked as a standalone Nexus operation (StartNexusOperationExecution +
// PollNexusOperationExecution) through Client - normally the proxy's
// upstream connection, so the upstream credentials are reused.
//
// The operation receives a toolpolicypb.VerifyToolCallsRequest as a
// "json/protobuf" payload and must complete (synchronously, or
// asynchronously within Timeout) with a toolpolicypb.VerifyToolCallsResponse
// payload. allowed=false denies the call with the given reason. A failed
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
	Operation string
	Timeout   time.Duration
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

	ctx, cancel := context.WithTimeout(ctx, v.Timeout)
	defer cancel()

	operationID, err := randomID()
	if err != nil {
		return UnavailableError{Err: err}
	}
	requestID, err := randomID()
	if err != nil {
		return UnavailableError{Err: err}
	}

	startResp, err := v.Client.StartNexusOperationExecution(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		Namespace:              v.Namespace,
		Identity:               "temporal-proxy",
		RequestId:              requestID,
		OperationId:            operationID,
		Endpoint:               v.Endpoint,
		Service:                v.Service,
		Operation:              v.Operation,
		ScheduleToCloseTimeout: durationpb.New(v.Timeout),
		Input:                  input,
	})
	if err != nil {
		return UnavailableError{Err: fmt.Errorf("starting nexus operation: %w", err)}
	}

	for {
		pollResp, err := v.Client.PollNexusOperationExecution(ctx, &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   v.Namespace,
			OperationId: operationID,
			RunId:       startResp.GetRunId(),
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
		})
		if err != nil {
			return UnavailableError{Err: fmt.Errorf("polling nexus operation: %w", err)}
		}
		if pollResp.GetWaitStage() != enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED {
			// The long poll returned before the operation closed; poll again
			// until it does or ctx expires.
			if err := ctx.Err(); err != nil {
				return UnavailableError{Err: err}
			}
			continue
		}
		if f := pollResp.GetFailure(); f != nil {
			return UnavailableError{Err: fmt.Errorf("nexus operation failed: %s", f.GetMessage())}
		}
		return decodeVerifyResponse(pollResp.GetResult())
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
}

type wireToolCall struct {
	Name       string            `json:"name,omitempty"`
	Parameters []json.RawMessage `json:"parameters,omitempty"`
}

func encodeVerifyRequest(req Request) (*commonpb.Payload, error) {
	wire := wireRequest{
		Caller: wireCaller{
			Namespace:  req.Identity.Namespace,
			Subject:    req.Identity.Subject,
			TaskQueue:  req.TaskQueue,
			WorkflowID: req.WorkflowID,
		},
	}
	for _, call := range req.Calls {
		wire.ToolCalls = append(wire.ToolCalls, wireToolCall{Name: call.Name, Parameters: call.Parameters})
	}
	data, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("encoding verifier request: %w", err)
	}
	return &commonpb.Payload{
		Metadata: map[string][]byte{
			metadataEncoding:    []byte(encodingJSONProto),
			metadataMessageType: []byte((&toolpolicypb.VerifyToolCallsRequest{}).ProtoReflect().Descriptor().FullName()),
		},
		Data: data,
	}, nil
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
	if !out.GetAllowed() {
		reason := out.GetReason()
		if reason == "" {
			reason = "tool calls rejected by tool verifier"
		}
		return DeniedError{Reason: reason}
	}
	return nil
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generating operation id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
