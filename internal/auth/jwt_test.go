package auth

import (
	"context"
	"errors"
	"testing"
)

// newJWTAuthenticator builds a JWTAuthenticator from the "emails" section of
// the given file with an injected validate func, bypassing real Google token
// verification.
func newJWTAuthenticator(t *testing.T, path string, validate func(context.Context, string) (string, error)) *JWTAuthenticator {
	t.Helper()
	cfg, err := loadAuthFile(path)
	if err != nil {
		t.Fatalf("loadAuthFile: %v", err)
	}
	identities, err := buildIdentities(cfg.Emails, false)
	if err != nil {
		t.Fatalf("buildIdentities: %v", err)
	}
	return &JWTAuthenticator{identities: identities, validate: validate}
}

// staticEmail returns a validate func that always resolves to the same email.
func staticEmail(email string) func(context.Context, string) (string, error) {
	return func(context.Context, string) (string, error) { return email, nil }
}

func TestJWTAuthenticator_ValidEmail(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"emails": {
			"sa@project.iam.gserviceaccount.com": {"namespace": "default", "task_queue": "proxy-test-queue", "subject": "worker-fleet-a"}
		}
	}`)

	a := newJWTAuthenticator(t, path, staticEmail("sa@project.iam.gserviceaccount.com"))

	identity, err := a.Authenticate(context.Background(), "any-token")
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

func TestJWTAuthenticator_UnknownEmail(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"emails": {
			"sa@project.iam.gserviceaccount.com": {"namespace": "default", "task_queue": "proxy-test-queue"}
		}
	}`)

	a := newJWTAuthenticator(t, path, staticEmail("stranger@example.com"))

	identity, err := a.Authenticate(context.Background(), "any-token")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if identity.Valid {
		t.Fatalf("expected invalid identity for unknown email, got %+v", identity)
	}
}

func TestJWTAuthenticator_ValidationError(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"emails": {
			"sa@project.iam.gserviceaccount.com": {"namespace": "default", "task_queue": "proxy-test-queue"}
		}
	}`)

	failing := func(context.Context, string) (string, error) {
		return "", errors.New("bad signature")
	}
	a := newJWTAuthenticator(t, path, failing)

	identity, err := a.Authenticate(context.Background(), "any-token")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if identity.Valid {
		t.Fatalf("expected invalid identity when token validation fails, got %+v", identity)
	}
}

func TestJWTAuthenticator_SubjectDefaultsToEmail(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"emails": {
			"sa@project.iam.gserviceaccount.com": {"namespace": "default", "task_queue": "proxy-test-queue"}
		}
	}`)

	a := newJWTAuthenticator(t, path, staticEmail("sa@project.iam.gserviceaccount.com"))

	identity, _ := a.Authenticate(context.Background(), "any-token")
	if identity.Subject != "sa@project.iam.gserviceaccount.com" {
		t.Fatalf("expected subject to default to email, got %q", identity.Subject)
	}
}

// TestJWTAuthenticator_IgnoresKeysSection confirms the JWT authenticator reads
// only the "emails" section of the unified file, not "keys".
func TestJWTAuthenticator_IgnoresKeysSection(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"keys":   {"some-api-key": {"namespace": "default", "task_queue": "key-queue"}},
		"emails": {"sa@project.iam.gserviceaccount.com": {"namespace": "default", "task_queue": "email-queue"}}
	}`)

	a := newJWTAuthenticator(t, path, staticEmail("sa@project.iam.gserviceaccount.com"))

	identity, _ := a.Authenticate(context.Background(), "any-token")
	if identity.TaskQueue != "email-queue" {
		t.Fatalf("expected email-queue from emails section, got %q", identity.TaskQueue)
	}
}

func TestJWTAuthenticator_MissingField(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"emails": {"sa@project.iam.gserviceaccount.com": {"namespace": "default"}}
	}`)

	if _, err := loadAuthFile(path); err != nil {
		t.Fatalf("loadAuthFile: %v", err)
	}
	cfg, _ := loadAuthFile(path)
	if _, err := buildIdentities(cfg.Emails, false); err == nil {
		t.Fatalf("expected error for email entry missing task_queue")
	}
}

func TestNewJWTAuthenticatorFromFile_RequiresAudience(t *testing.T) {
	path := writeStaticAuthFile(t, `{"emails": {}}`)
	if _, err := NewJWTAuthenticatorFromFile(context.Background(), path, ""); err == nil {
		t.Fatalf("expected error when audience is empty")
	}
}

// TestNewJWTAuthenticatorFromFile_Constructs verifies the authenticator (and
// its Google token validator) builds without network access or ambient
// credentials - Google's certs are only fetched lazily at Validate time.
func TestNewJWTAuthenticatorFromFile_Constructs(t *testing.T) {
	path := writeStaticAuthFile(t, `{
		"emails": {"sa@project.iam.gserviceaccount.com": {"namespace": "default", "task_queue": "proxy-test-queue"}}
	}`)

	a, err := NewJWTAuthenticatorFromFile(context.Background(), path, "https://proxy.example.com")
	if err != nil {
		t.Fatalf("NewJWTAuthenticatorFromFile: %v", err)
	}
	if len(a.identities) != 1 {
		t.Fatalf("expected 1 loaded email identity, got %d", len(a.identities))
	}
}
