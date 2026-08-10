package auth

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func writeStaticAuthFile(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "static-auth.json")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("writing static auth file: %v", err)
	}
	return path
}

func TestStaticAuthenticator_ValidKey(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"keys": {
			"testkey123": {"namespace": "default", "task_queue": "proxy-test-queue", "subject": "worker-fleet-a"}
		}
	}`)

	a, err := NewStaticAuthenticatorFromFile(path)
	if err != nil {
		t.Fatalf("NewStaticAuthenticatorFromFile: %v", err)
	}

	identity, err := a.Authenticate(context.Background(), "testkey123")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if !identity.Valid {
		t.Fatalf("expected valid identity, got %+v", identity)
	}
	if identity.Namespace != "default" || identity.TaskQueue != "proxy-test-queue" || identity.Subject != "worker-fleet-a" {
		t.Fatalf("unexpected identity: %+v", identity)
	}
}

func TestStaticAuthenticator_UnknownKey(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"keys": {
			"testkey123": {"namespace": "default", "task_queue": "proxy-test-queue"}
		}
	}`)

	a, err := NewStaticAuthenticatorFromFile(path)
	if err != nil {
		t.Fatalf("NewStaticAuthenticatorFromFile: %v", err)
	}

	identity, err := a.Authenticate(context.Background(), "does-not-exist")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if identity.Valid {
		t.Fatalf("expected invalid identity, got %+v", identity)
	}
}

func TestStaticAuthenticator_TwoDistinctKeys(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"keys": {
			"key-a": {"namespace": "default", "task_queue": "queue-a"},
			"key-b": {"namespace": "default", "task_queue": "queue-b"}
		}
	}`)

	a, err := NewStaticAuthenticatorFromFile(path)
	if err != nil {
		t.Fatalf("NewStaticAuthenticatorFromFile: %v", err)
	}

	idA, _ := a.Authenticate(context.Background(), "key-a")
	idB, _ := a.Authenticate(context.Background(), "key-b")

	if idA.TaskQueue != "queue-a" || idB.TaskQueue != "queue-b" {
		t.Fatalf("keys resolved to wrong queues: idA=%+v idB=%+v", idA, idB)
	}
}

func TestStaticAuthenticator_MissingField(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"keys": {
			"key-a": {"namespace": "default"}
		}
	}`)

	if _, err := NewStaticAuthenticatorFromFile(path); err == nil {
		t.Fatalf("expected error for entry missing task_queue")
	}
}
