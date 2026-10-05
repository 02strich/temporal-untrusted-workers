package toolpolicy

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"google.golang.org/protobuf/proto"
)

func jsonPlainPayload(data string) *commonpb.Payload {
	return &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(data)}
}

func nexusCommand(input *commonpb.Payload) *commandpb.Command {
	return &commandpb.Command{
		Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
			ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
				Endpoint: "payments", Service: "billing", Operation: "Charge", Input: input,
			},
		},
	}
}

func activityCommand(taskQueue string, inputs ...*commonpb.Payload) *commandpb.Command {
	attrs := &commandpb.ScheduleActivityTaskCommandAttributes{
		ActivityType: &commonpb.ActivityType{Name: "SendEmail"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
	}
	if len(inputs) > 0 {
		attrs.Input = &commonpb.Payloads{Payloads: inputs}
	}
	return &commandpb.Command{
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: attrs},
	}
}

// decodeJSON parses a rendered parameter for comparison.
func decodeJSON(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("parameter is not a JSON object: %s (%v)", raw, err)
	}
	return got
}

func TestFromCommands_SelectsToolCalls(t *testing.T) {
	commands := []*commandpb.Command{
		activityCommand("queue-a"), // own queue: not a tool call
		nexusCommand(nil),
		activityCommand("trusted-tools"),
		activityCommand(""),
		{Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
			RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{ScheduledEventId: 5},
		}},
		{Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{}}},
	}

	calls := FromCommands(commands, "queue-a")
	type summary struct {
		kind   Kind
		name   string
		target string
		params int
	}
	var got []summary
	for _, c := range calls {
		if c.Err != nil {
			t.Fatalf("unexpected error for %s: %v", c.Name, c.Err)
		}
		got = append(got, summary{c.Kind, c.Name, c.TargetTaskQueue, len(c.Parameters)})
	}
	want := []summary{
		{KindNexusOperation, "payments/billing/Charge", "", 0},
		{KindActivity, "trusted-tools/SendEmail", "trusted-tools", 0},
		{KindActivity, "/SendEmail", "", 0},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	if calls := FromCommands([]*commandpb.Command{activityCommand("queue-a")}, "queue-a"); len(calls) != 0 {
		t.Fatalf("same-queue activity must not be a tool call, got %+v", calls)
	}
}

func TestFromCommands_ActivityArguments(t *testing.T) {
	calls := FromCommands([]*commandpb.Command{
		activityCommand("trusted-tools", jsonPlainPayload(`"a@example.com"`), jsonPlainPayload(`{"subject":"hi"}`)),
	}, "queue-a")
	if len(calls) != 1 || len(calls[0].Parameters) != 2 {
		t.Fatalf("expected one call with two parameters, got %+v", calls)
	}
	if got := decodeJSON(t, calls[0].Parameters[0]); got["value"] != "a@example.com" {
		t.Fatalf("unexpected first parameter: %v", got)
	}
	if got := decodeJSON(t, calls[0].Parameters[1]); !reflect.DeepEqual(got["value"], map[string]any{"subject": "hi"}) {
		t.Fatalf("unexpected second parameter: %v", got)
	}
}

func TestPayloadParameter(t *testing.T) {
	temporalMsg := &commonpb.WorkflowType{Name: "Child"}
	temporalBytes, err := proto.Marshal(temporalMsg)
	if err != nil {
		t.Fatal(err)
	}
	payload := func(encoding, messageType, data string) *commonpb.Payload {
		md := map[string][]byte{"encoding": []byte(encoding)}
		if messageType != "" {
			md["messageType"] = []byte(messageType)
		}
		return &commonpb.Payload{Metadata: md, Data: []byte(data)}
	}

	tests := []struct {
		name    string
		payload *commonpb.Payload
		want    map[string]any
	}{
		{
			name:    "json/protobuf message is spliced into a typed Any",
			payload: payload("json/protobuf", "acme.billing.v1.ChargeRequest", `{"amount":"500","currency":"USD"}`),
			want:    map[string]any{"@type": "type.googleapis.com/acme.billing.v1.ChargeRequest", "amount": "500", "currency": "USD"},
		},
		{
			name:    "json/protobuf well-known type uses the value form",
			payload: payload("json/protobuf", "google.protobuf.Struct", `{"a":1}`),
			want:    map[string]any{"@type": "type.googleapis.com/google.protobuf.Struct", "value": map[string]any{"a": float64(1)}},
		},
		{
			name:    "json/protobuf non-object uses the value form",
			payload: payload("json/protobuf", "acme.v1.Weird", `"x"`),
			want:    map[string]any{"@type": "type.googleapis.com/acme.v1.Weird", "value": "x"},
		},
		{
			name:    "json/protobuf without messageType becomes a Value",
			payload: payload("json/protobuf", "", `{"a":"b"}`),
			want:    map[string]any{"@type": "type.googleapis.com/google.protobuf.Value", "value": map[string]any{"a": "b"}},
		},
		{
			name:    "json/plain becomes a Value",
			payload: payload("json/plain", "", `{"amount":500}`),
			want:    map[string]any{"@type": "type.googleapis.com/google.protobuf.Value", "value": map[string]any{"amount": float64(500)}},
		},
		{
			name:    "known binary/protobuf type is rendered",
			payload: &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/protobuf"), "messageType": []byte("temporal.api.common.v1.WorkflowType")}, Data: temporalBytes},
			want:    map[string]any{"@type": "type.googleapis.com/temporal.api.common.v1.WorkflowType", "name": "Child"},
		},
		{
			name:    "binary/plain becomes BytesValue",
			payload: payload("binary/plain", "", "hi"),
			want:    map[string]any{"@type": "type.googleapis.com/google.protobuf.BytesValue", "value": "aGk="},
		},
		{
			name:    "binary/null becomes a null Value",
			payload: payload("binary/null", "", ""),
			want:    map[string]any{"@type": "type.googleapis.com/google.protobuf.Value", "value": nil},
		},
		{
			name:    "opaque encodings pack the Payload",
			payload: payload("binary/encrypted", "", "secret"),
			want: map[string]any{
				"@type":    "type.googleapis.com/temporal.api.common.v1.Payload",
				"metadata": map[string]any{"encoding": "YmluYXJ5L2VuY3J5cHRlZA=="},
				"data":     "c2VjcmV0",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := payloadParameter(tc.payload)
			if err != nil {
				t.Fatalf("payloadParameter: %v", err)
			}
			if got := decodeJSON(t, raw); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPayloadParameter_Unrenderable(t *testing.T) {
	for name, p := range map[string]*commonpb.Payload{
		"unknown binary/protobuf type": {Metadata: map[string][]byte{"encoding": []byte("binary/protobuf"), "messageType": []byte("acme.v1.Secret")}, Data: []byte{1}},
		"binary/protobuf without type": {Metadata: map[string][]byte{"encoding": []byte("binary/protobuf")}, Data: []byte{1}},
		"invalid json/plain":           jsonPlainPayload(`{nope`),
		"json/protobuf with @type":     {Metadata: map[string][]byte{"encoding": []byte("json/protobuf"), "messageType": []byte("acme.v1.X")}, Data: []byte(`{"@type":"y"}`)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := payloadParameter(p); err == nil {
				t.Fatalf("expected an error")
			}
		})
	}

	calls := FromCommands([]*commandpb.Command{nexusCommand(&commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("binary/protobuf"), "messageType": []byte("acme.v1.Secret")},
		Data:     []byte{1},
	})}, "queue-a")
	if len(calls) != 1 || calls[0].Err == nil || len(calls[0].Parameters) != 1 {
		t.Fatalf("expected the call to carry the error, got %+v", calls)
	}
}

func TestDefault(t *testing.T) {
	nexus := FromCommands([]*commandpb.Command{nexusCommand(&commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("binary/protobuf"), "messageType": []byte("acme.v1.Secret")},
	})}, "queue-a")
	if err := (Default{}).Verify(context.Background(), Request{TaskQueue: "queue-a", Calls: nexus}); err != nil {
		t.Fatalf("Default must allow Nexus calls (even unrenderable ones), got %v", err)
	}

	activity := FromCommands([]*commandpb.Command{activityCommand("trusted-tools")}, "queue-a")
	err := (Default{}).Verify(context.Background(), Request{TaskQueue: "queue-a", Calls: activity})
	var denied DeniedError
	if !errors.As(err, &denied) {
		t.Fatalf("Default must deny cross-queue activities, got %v", err)
	}
	if want := `activity "trusted-tools/SendEmail" targets task queue "trusted-tools", not authorized queue "queue-a"`; denied.Reason != want {
		t.Fatalf("reason %q, want %q", denied.Reason, want)
	}
}
