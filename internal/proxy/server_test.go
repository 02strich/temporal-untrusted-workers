package proxy

import (
	"testing"

	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/workflowservice/v1"
)

func TestDisableWorkerCommandsCapability(t *testing.T) {
	resp := &workflowservice.DescribeNamespaceResponse{
		NamespaceInfo: &namespacepb.NamespaceInfo{
			Capabilities: &namespacepb.NamespaceInfo_Capabilities{
				WorkerCommands:    true,
				WorkerHeartbeats:  true,
				SyncUpdate:        true,
				PollerAutoscaling: true,
			},
		},
	}

	disableWorkerCommandsCapability(resp)

	caps := resp.GetNamespaceInfo().GetCapabilities()
	if caps.GetWorkerCommands() {
		t.Fatalf("expected worker commands capability to be disabled")
	}
	if !caps.GetWorkerHeartbeats() || !caps.GetSyncUpdate() || !caps.GetPollerAutoscaling() {
		t.Fatalf("expected unrelated capabilities to remain enabled: %+v", caps)
	}
}

func TestDisableWorkerCommandsCapabilityKeepsDisabledCapabilityDisabled(t *testing.T) {
	resp := &workflowservice.DescribeNamespaceResponse{
		NamespaceInfo: &namespacepb.NamespaceInfo{
			Capabilities: &namespacepb.NamespaceInfo_Capabilities{
				WorkerCommands: false,
			},
		},
	}

	disableWorkerCommandsCapability(resp)

	if resp.GetNamespaceInfo().GetCapabilities().GetWorkerCommands() {
		t.Fatalf("expected worker commands capability to remain disabled")
	}
}

func TestDisableWorkerCommandsCapabilityHandlesMissingFields(t *testing.T) {
	cases := []struct {
		name string
		resp *workflowservice.DescribeNamespaceResponse
	}{
		{name: "nil response"},
		{name: "nil namespace info", resp: &workflowservice.DescribeNamespaceResponse{}},
		{
			name: "nil capabilities",
			resp: &workflowservice.DescribeNamespaceResponse{
				NamespaceInfo: &namespacepb.NamespaceInfo{},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			disableWorkerCommandsCapability(tc.resp)
		})
	}
}
