// Package auth defines the pluggable identity model used to authorize
// downstream (worker-facing) calls to the proxy.
package auth

import "context"

// Identity is the result of resolving an API key presented by a downstream
// caller. A valid Identity pins the caller to exactly one namespace and task
// queue; the proxy rejects any call that would touch anything else.
type Identity struct {
	Valid     bool
	Namespace string
	TaskQueue string
	// Subject is an optional human-readable identifier for the credential,
	// used only for logging/audit.
	Subject string
}

// Authenticator resolves an API key extracted from incoming request
// metadata into an Identity. Implementations are free to back this with a
// static config, a database, a secrets manager, etc. - the proxy only
// depends on this interface.
type Authenticator interface {
	Authenticate(ctx context.Context, apiKey string) (Identity, error)
}
