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

// authFileConfig is the JSON shape of the unified auth file. "keys" maps raw
// API keys to their settings (used by StaticAuthenticator); "emails" maps
// verified email addresses to the same settings (used by JWTAuthenticator).
// Both sections are optional, and a deployment uses only the one that matches
// its configured auth mode.
//
//	{
//	  "keys":   {"wk_live_abc123...":        {"namespace": "default", "task_queue": "my-task-queue", "subject": "worker-fleet-a"}},
//	  "emails": {"sa@project.iam.gserviceaccount.com": {"namespace": "default", "task_queue": "my-task-queue", "subject": "worker-fleet-a"}}
//	}
type authFileConfig struct {
	Keys   map[string]staticKeyEntry `json:"keys"`
	Emails map[string]staticKeyEntry `json:"emails"`
}

// loadAuthFile reads and parses the unified auth file at path.
func loadAuthFile(path string) (authFileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return authFileConfig{}, fmt.Errorf("auth: reading auth file: %w", err)
	}

	var cfg authFileConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return authFileConfig{}, fmt.Errorf("auth: parsing auth file: %w", err)
	}
	return cfg, nil
}

// buildIdentities turns a section of auth-file entries into resolved
// Identities. The returned map is keyed by sha256(lookupKey) hex when hash is
// true (for secret API keys, so raw keys aren't resident as map keys) and by
// the raw lookup key otherwise (for non-secret email addresses).
func buildIdentities(entries map[string]staticKeyEntry, hash bool) (map[string]Identity, error) {
	identities := make(map[string]Identity, len(entries))
	for lookupKey, entry := range entries {
		if entry.Namespace == "" || entry.TaskQueue == "" {
			return nil, fmt.Errorf("auth: auth file: entry %q is missing namespace or task_queue", lookupKey)
		}
		mapKey := lookupKey
		if hash {
			mapKey = hashKey(lookupKey)
		}
		identities[mapKey] = Identity{
			Valid:     true,
			Namespace: entry.Namespace,
			TaskQueue: entry.TaskQueue,
			Subject:   entry.Subject,
		}
	}
	return identities, nil
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

// NewStaticAuthenticatorFromFile loads the "keys" section of the unified auth
// JSON file.
func NewStaticAuthenticatorFromFile(path string) (*StaticAuthenticator, error) {
	cfg, err := loadAuthFile(path)
	if err != nil {
		return nil, err
	}

	identities, err := buildIdentities(cfg.Keys, true)
	if err != nil {
		return nil, err
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
