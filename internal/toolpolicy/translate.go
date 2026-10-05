package toolpolicy

import (
	"encoding/json"
	"errors"
	"fmt"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const typeURLPrefix = "type.googleapis.com/"

// Payload metadata, as written by the Temporal SDKs' payload converters.
const (
	metadataEncoding    = "encoding"
	metadataMessageType = "messageType"

	encodingJSONProto   = "json/protobuf"
	encodingBinaryProto = "binary/protobuf"
	encodingJSONPlain   = "json/plain"
	encodingBinaryPlain = "binary/plain"
	encodingBinaryNull  = "binary/null"
)

// FromCommands translates the commands of a RespondWorkflowTaskCompleted call
// that leave taskQueue - the task queue that issued the workflow task token -
// into tool calls:
//
//   - ScheduleNexusOperation becomes "<endpoint>/<service>/<operation>", with
//     its input (if any) as the only parameter.
//   - ScheduleActivityTask on a task queue other than taskQueue becomes
//     "<taskQueue>/<activityType>", with one parameter per input argument.
//
// Activities on taskQueue and all other commands are not tool calls.
func FromCommands(commands []*commandpb.Command, taskQueue string) []Call {
	var calls []Call
	for _, cmd := range commands {
		switch attr := cmd.GetAttributes().(type) {
		case *commandpb.Command_ScheduleNexusOperationCommandAttributes:
			a := attr.ScheduleNexusOperationCommandAttributes
			call := Call{
				Kind: KindNexusOperation,
				Name: a.GetEndpoint() + "/" + a.GetService() + "/" + a.GetOperation(),
			}
			if a.GetInput() != nil {
				call.addParameter(a.GetInput())
			}
			calls = append(calls, call)
		case *commandpb.Command_ScheduleActivityTaskCommandAttributes:
			a := attr.ScheduleActivityTaskCommandAttributes
			target := a.GetTaskQueue().GetName()
			if target == taskQueue {
				continue
			}
			call := Call{
				Kind:            KindActivity,
				Name:            target + "/" + a.GetActivityType().GetName(),
				TargetTaskQueue: target,
			}
			for _, p := range a.GetInput().GetPayloads() {
				call.addParameter(p)
			}
			calls = append(calls, call)
		}
	}
	return calls
}

func (c *Call) addParameter(p *commonpb.Payload) {
	param, err := payloadParameter(p)
	if err != nil {
		if c.Err == nil {
			c.Err = fmt.Errorf("tool call %q parameter %d: %w", c.Name, len(c.Parameters), err)
		}
		param = nil
	}
	c.Parameters = append(c.Parameters, param)
}

// payloadParameter renders an input payload as the protobuf JSON form of a
// google.protobuf.Any, typed where possible. It never needs the descriptor of
// a user type: json/protobuf payloads already are that type's JSON form, so
// the "@type" is spliced in; binary/protobuf payloads can only be rendered if
// the proxy itself links in their type.
func payloadParameter(p *commonpb.Payload) (json.RawMessage, error) {
	encoding := string(p.GetMetadata()[metadataEncoding])
	messageType := string(p.GetMetadata()[metadataMessageType])
	data := p.GetData()

	switch {
	case encoding == encodingJSONProto && messageType != "":
		return spliceJSONAny(messageType, data)
	case encoding == encodingJSONProto || encoding == encodingJSONPlain:
		value := &structpb.Value{}
		if err := protojson.Unmarshal(data, value); err != nil {
			return nil, fmt.Errorf("decoding %s payload: %w", encoding, err)
		}
		return marshalAny(value)
	case encoding == encodingBinaryProto:
		if messageType == "" {
			return nil, errors.New("binary/protobuf payload without messageType cannot be verified")
		}
		mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(messageType))
		if err != nil {
			return nil, fmt.Errorf("binary/protobuf payload of unknown type %q cannot be verified; send it as json/protobuf", messageType)
		}
		msg := mt.New().Interface()
		if err := proto.Unmarshal(data, msg); err != nil {
			return nil, fmt.Errorf("decoding binary/protobuf payload of type %q: %w", messageType, err)
		}
		return marshalAny(msg)
	case encoding == encodingBinaryPlain:
		return marshalAny(wrapperspb.Bytes(data))
	case encoding == encodingBinaryNull:
		return marshalAny(structpb.NewNullValue())
	default:
		// Opaque to the proxy (e.g. encrypted by a payload codec): pass the
		// payload itself, so policies can still decide by tool name.
		return marshalAny(p)
	}
}

func marshalAny(msg proto.Message) (json.RawMessage, error) {
	a, err := anypb.New(msg)
	if err != nil {
		return nil, err
	}
	return protojson.Marshal(a)
}

// specialJSONTypes are the well-known types whose protobuf JSON form is not a
// plain object of fields; inside an Any they appear under a "value" key.
var specialJSONTypes = map[string]bool{
	"google.protobuf.Any":         true,
	"google.protobuf.Struct":      true,
	"google.protobuf.Value":       true,
	"google.protobuf.ListValue":   true,
	"google.protobuf.Timestamp":   true,
	"google.protobuf.Duration":    true,
	"google.protobuf.FieldMask":   true,
	"google.protobuf.DoubleValue": true,
	"google.protobuf.FloatValue":  true,
	"google.protobuf.Int64Value":  true,
	"google.protobuf.UInt64Value": true,
	"google.protobuf.Int32Value":  true,
	"google.protobuf.UInt32Value": true,
	"google.protobuf.BoolValue":   true,
	"google.protobuf.StringValue": true,
	"google.protobuf.BytesValue":  true,
}

// spliceJSONAny builds the protobuf JSON form of an Any holding messageType,
// whose own JSON form is data.
func spliceJSONAny(messageType string, data []byte) (json.RawMessage, error) {
	if !json.Valid(data) {
		return nil, errors.New("json/protobuf payload is not valid JSON")
	}
	typeURL, err := json.Marshal(typeURLPrefix + messageType)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if specialJSONTypes[messageType] || json.Unmarshal(data, &fields) != nil {
		return json.Marshal(map[string]json.RawMessage{"@type": typeURL, "value": data})
	}
	if _, ok := fields["@type"]; ok {
		return nil, errors.New(`json/protobuf payload already has an "@type" field`)
	}
	if fields == nil {
		// JSON null.
		return json.Marshal(map[string]json.RawMessage{"@type": typeURL, "value": data})
	}
	fields["@type"] = typeURL
	return json.Marshal(fields)
}
