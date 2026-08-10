package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
)

// staticKeyEntry is the JSON shape of one entry in a static auth file.
type staticKeyEntry struct {
	Namespace string `json:"namespace"`
	TaskQueue string `json:"task_queue"`
	Subject   string `json:"subject"`
}

// staticConfig is the JSON shape of a static auth file:
//
//	{
//	  "keys": {
//	    "wk_live_abc123...": {"namespace": "default", "task_queue": "my-task-queue", "subject": "worker-fleet-a"}
//	  }
//	}
type staticConfig struct {
	Keys map[string]staticKeyEntry `json:"keys"`
}

// StaticAuthenticator authenticates API keys against a fixed, file-loaded
// table. It is the Authenticator implementation shipped out of the box;
// production deployments may prefer a database- or secrets-manager-backed
// implementation instead, since this loads the whole key table into memory
// at startup and never refreshes it.
type StaticAuthenticator struct {
	// keyed by sha256(apiKey) hex, so raw keys aren't resident as map keys.
	identities map[string]Identity
}

// NewStaticAuthenticatorFromFile loads a static auth JSON file.
func NewStaticAuthenticatorFromFile(path string) (*StaticAuthenticator, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("auth: reading static auth file: %w", err)
	}

	var cfg staticConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("auth: parsing static auth file: %w", err)
	}

	identities := make(map[string]Identity, len(cfg.Keys))
	for key, entry := range cfg.Keys {
		if entry.Namespace == "" || entry.TaskQueue == "" {
			return nil, fmt.Errorf("auth: static auth file: entry for a key is missing namespace or task_queue")
		}
		identities[hashKey(key)] = Identity{
			Valid:     true,
			Namespace: entry.Namespace,
			TaskQueue: entry.TaskQueue,
			Subject:   entry.Subject,
		}
	}

	return &StaticAuthenticator{identities: identities}, nil
}

// Authenticate implements Authenticator.
func (s *StaticAuthenticator) Authenticate(_ context.Context, apiKey string) (Identity, error) {
	identity, ok := s.identities[hashKey(apiKey)]
	if !ok {
		return Identity{}, nil
	}
	return identity, nil
}

func hashKey(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(sum[:])
}
