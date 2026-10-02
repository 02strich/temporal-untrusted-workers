package commandpolicy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	commandpolicypb "github.com/02strich/temporal-untrusted-workers/gen/commandpolicy/v1"
)

// DefaultNexusOperation is the operation name NexusVerifier calls when none
// is configured.
const DefaultNexusOperation = "VerifyCommands"

// Payload encodings and metadata keys, matching the Temporal SDKs' protobuf
// payload converters.
const (
	metadataEncoding    = "encoding"
	metadataMessageType = "messageType"
	encodingJSONProto   = "json/protobuf"
	encodingBinaryProto = "binary/protobuf"
)

// NexusVerifier delegates command verification to a Temporal Nexus service,
// invoked as a standalone Nexus operation (StartNexusOperationExecution +
// PollNexusOperationExecution) through Client - normally the proxy's
// upstream connection, so the upstream credentials are reused.
//
// The operation receives a commandpolicypb.VerifyCommandsRequest as a
// "json/protobuf" payload and must complete (synchronously, or
// asynchronously within Timeout) with a commandpolicypb.VerifyCommandsResponse
// payload. allowed=false denies the call with the given reason. A failed
// operation, a malformed result, or no result within Timeout makes the proxy
// fail closed with Unavailable.
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

func encodeVerifyRequest(req Request) (*commonpb.Payload, error) {
	msg := &commandpolicypb.VerifyCommandsRequest{
		Namespace:  req.Identity.Namespace,
		TaskQueue:  req.TaskQueue,
		TaskQueues: req.Identity.TaskQueues,
		Subject:    req.Identity.Subject,
		Commands:   req.Commands,
	}
	data, err := protojson.Marshal(msg)
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

func decodeVerifyResponse(result *commonpb.Payload) error {
	if result == nil {
		return UnavailableError{Err: errors.New("nexus operation returned no result")}
	}
	var out commandpolicypb.VerifyCommandsResponse
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
			reason = "commands rejected by command verifier"
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
