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
			"testkey123": {"namespace": "default", "task_queues": ["proxy-test-queue"], "subject": "worker-fleet-a"}
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
	if identity.Namespace != "default" || !identity.AllowsTaskQueue("proxy-test-queue") || identity.Subject != "worker-fleet-a" {
		t.Fatalf("unexpected identity: %+v", identity)
	}
}

func TestStaticAuthenticator_MultipleTaskQueues(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"keys": {
			"testkey123": {"namespace": "default", "task_queues": ["queue-a", "queue-b"]}
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
	if !identity.AllowsTaskQueue("queue-a") || !identity.AllowsTaskQueue("queue-b") || identity.AllowsTaskQueue("queue-c") {
		t.Fatalf("unexpected task queue authorization: %+v", identity)
	}
}

func TestStaticAuthenticator_UnknownKey(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"keys": {
			"testkey123": {"namespace": "default", "task_queues": ["proxy-test-queue"]}
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
			"key-a": {"namespace": "default", "task_queues": ["queue-a"]},
			"key-b": {"namespace": "default", "task_queues": ["queue-b"]}
		}
	}`)

	a, err := NewStaticAuthenticatorFromFile(path)
	if err != nil {
		t.Fatalf("NewStaticAuthenticatorFromFile: %v", err)
	}

	idA, _ := a.Authenticate(context.Background(), "key-a")
	idB, _ := a.Authenticate(context.Background(), "key-b")

	if !idA.AllowsTaskQueue("queue-a") || !idB.AllowsTaskQueue("queue-b") {
		t.Fatalf("keys resolved to wrong queues: idA=%+v idB=%+v", idA, idB)
	}
}

// TestStaticAuthenticator_IgnoresEmailsSection confirms the static
// authenticator reads only the "keys" section of the unified file, not
// "emails".
func TestStaticAuthenticator_IgnoresEmailsSection(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"keys":   {"key-a": {"namespace": "default", "task_queues": ["key-queue"]}},
		"emails": {"sa@project.iam.gserviceaccount.com": {"namespace": "default", "task_queues": ["email-queue"]}}
	}`)

	a, err := NewStaticAuthenticatorFromFile(path)
	if err != nil {
		t.Fatalf("NewStaticAuthenticatorFromFile: %v", err)
	}

	id, _ := a.Authenticate(context.Background(), "key-a")
	if !id.Valid || !id.AllowsTaskQueue("key-queue") {
		t.Fatalf("expected key-queue from keys section, got %+v", id)
	}

	// The email address is not a valid API key.
	if idEmail, _ := a.Authenticate(context.Background(), "sa@project.iam.gserviceaccount.com"); idEmail.Valid {
		t.Fatalf("email address must not authenticate as a static key: %+v", idEmail)
	}
}

func TestStaticAuthenticator_MissingField(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"keys": {
			"key-a": {"namespace": "default"}
		}
	}`)

	if _, err := NewStaticAuthenticatorFromFile(path); err == nil {
		t.Fatalf("expected error for entry missing task_queues")
	}
}

func TestStaticAuthenticator_RejectsOldTaskQueueField(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"keys": {
			"key-a": {"namespace": "default", "task_queue": "queue-a"}
		}
	}`)

	if _, err := NewStaticAuthenticatorFromFile(path); err == nil {
		t.Fatalf("expected error for entry using old task_queue field")
	}
}

func TestStaticAuthenticator_RejectsEmptyTaskQueues(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"keys": {
			"key-a": {"namespace": "default", "task_queues": []}
		}
	}`)

	if _, err := NewStaticAuthenticatorFromFile(path); err == nil {
		t.Fatalf("expected error for empty task_queues")
	}
}

func TestStaticAuthenticator_RejectsEmptyTaskQueueName(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"keys": {
			"key-a": {"namespace": "default", "task_queues": ["queue-a", ""]}
		}
	}`)

	if _, err := NewStaticAuthenticatorFromFile(path); err == nil {
		t.Fatalf("expected error for empty task queue name")
	}
}
