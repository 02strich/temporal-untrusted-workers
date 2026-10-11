package inputevents

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/02strich/temporal-untrusted-workers/internal/kvstore"
)

// RunKey identifies a workflow run.
type RunKey struct {
	Namespace  string
	WorkflowID string
	RunID      string
}

func (k RunKey) String() string {
	return k.Namespace + "/" + k.WorkflowID + "/" + k.RunID
}

// StateStore keeps each run's RunState between polls. Implementations must be
// safe for concurrent use. A run has at most one outstanding workflow task, so
// callers don't need to serialize a Get with the Put that follows it.
type StateStore interface {
	Get(ctx context.Context, key RunKey) (RunState, bool, error)
	Put(ctx context.Context, key RunKey, state RunState) error
	Close() error
}

// LocalStore is an in-memory StateStore (TTL + LRU bounded). State is lost
// when the proxy restarts and isn't shared between proxy instances; the
// verifier then sees a run's later context without its beginning (the next
// batch has FromHistoryStart unset).
type LocalStore struct {
	local *kvstore.Local[RunState]
}

// NewLocalStore creates a LocalStore with the given sliding TTL and
// approximate maximum number of runs.
func NewLocalStore(ttl time.Duration, maxRuns int) *LocalStore {
	return &LocalStore{local: kvstore.NewLocal[RunState](ttl, maxRuns)}
}

func (s *LocalStore) Get(_ context.Context, key RunKey) (RunState, bool, error) {
	state, ok := s.local.Get(key.String())
	return state, ok, nil
}

func (s *LocalStore) Put(_ context.Context, key RunKey, state RunState) error {
	s.local.Put(key.String(), state)
	return nil
}

func (s *LocalStore) Close() error {
	return s.local.Close()
}

const valkeyKeyPrefix = "temporal-proxy:input-state:"

// ValkeyStore is a StateStore backed by Valkey, shared by all proxy instances
// using it and surviving proxy restarts.
type ValkeyStore struct {
	client kvstore.Client
	ttl    time.Duration
}

// NewValkeyStore returns a StateStore keeping run state in client with the
// given sliding TTL. The caller owns client: closing the store doesn't close
// it.
func NewValkeyStore(client kvstore.Client, ttl time.Duration) *ValkeyStore {
	return &ValkeyStore{client: client, ttl: ttl}
}

func (s *ValkeyStore) Get(ctx context.Context, key RunKey) (RunState, bool, error) {
	value, found, err := s.client.Get(ctx, valkeyKeyPrefix+key.String(), s.ttl)
	if err != nil {
		return RunState{}, false, fmt.Errorf("get valkey input state: %w", err)
	}
	if !found {
		return RunState{}, false, nil
	}
	var state RunState
	if err := json.Unmarshal([]byte(value), &state); err != nil {
		return RunState{}, false, fmt.Errorf("decode valkey input state: %w", err)
	}
	return state, true, nil
}

func (s *ValkeyStore) Put(ctx context.Context, key RunKey, state RunState) error {
	value, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode valkey input state: %w", err)
	}
	if err := s.client.Put(ctx, valkeyKeyPrefix+key.String(), string(value), s.ttl); err != nil {
		return fmt.Errorf("put valkey input state: %w", err)
	}
	return nil
}

// Close is a no-op: the client belongs to the caller, which may share it
// (e.g. with the token cache) and closes it itself.
func (s *ValkeyStore) Close() error {
	return nil
}
