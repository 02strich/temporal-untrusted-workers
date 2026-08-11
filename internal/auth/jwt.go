package auth

import (
	"context"
	"fmt"

	"google.golang.org/api/idtoken"
	"google.golang.org/api/option"
)

// JWTAuthenticator authenticates Google-signed identity tokens (JWTs)
// presented as the API key. It verifies the token's signature, audience, and
// expiry, then resolves the token's verified email claim against a fixed,
// file-loaded table of email -> Identity (the "emails" section of the unified
// auth file). Like StaticAuthenticator it loads the whole table into memory at
// startup and never refreshes it.
type JWTAuthenticator struct {
	// keyed by the raw email address (emails are identifiers, not secrets).
	identities map[string]Identity
	// validate verifies a token and returns its verified email. It is a field
	// so tests can inject a fake in place of real Google token verification.
	validate func(ctx context.Context, token string) (email string, err error)
}

// NewJWTAuthenticatorFromFile loads the "emails" section of the unified auth
// file and builds an authenticator that validates Google ID tokens against the
// given audience. The audience must match what workers request when minting
// their identity token; an empty audience is rejected because it would disable
// audience verification.
func NewJWTAuthenticatorFromFile(ctx context.Context, path, audience string) (*JWTAuthenticator, error) {
	if audience == "" {
		return nil, fmt.Errorf("auth: JWT authenticator requires a non-empty audience")
	}

	cfg, err := loadAuthFile(path)
	if err != nil {
		return nil, err
	}

	identities, err := buildIdentities(cfg.Emails, false)
	if err != nil {
		return nil, err
	}

	// The validator only needs Google's public certs (a public endpoint), so
	// it must not require ambient Google credentials.
	validator, err := idtoken.NewValidator(ctx, option.WithoutAuthentication())
	if err != nil {
		return nil, fmt.Errorf("auth: creating ID token validator: %w", err)
	}

	return &JWTAuthenticator{
		identities: identities,
		validate:   googleTokenValidator(validator, audience),
	}, nil
}

// Authenticate implements Authenticator. It returns an invalid (zero) Identity
// - not an error - whenever the token cannot be resolved to a configured
// identity, so the interceptor uniformly denies it the same way it denies an
// unknown static key.
func (a *JWTAuthenticator) Authenticate(ctx context.Context, apiKey string) (Identity, error) {
	email, err := a.validate(ctx, apiKey)
	if err != nil {
		return Identity{}, nil
	}

	identity, ok := a.identities[email]
	if !ok {
		return Identity{}, nil
	}
	if identity.Subject == "" {
		identity.Subject = email
	}
	return identity, nil
}

// googleTokenValidator returns a validate func that checks a Google ID token
// against audience and extracts its verified email claim. A token whose email
// is absent or unverified is rejected.
func googleTokenValidator(validator *idtoken.Validator, audience string) func(context.Context, string) (string, error) {
	return func(ctx context.Context, token string) (string, error) {
		payload, err := validator.Validate(ctx, token, audience)
		if err != nil {
			return "", err
		}

		verified, _ := payload.Claims["email_verified"].(bool)
		if !verified {
			return "", fmt.Errorf("auth: token email is not verified")
		}
		email, _ := payload.Claims["email"].(string)
		if email == "" {
			return "", fmt.Errorf("auth: token has no email claim")
		}
		return email, nil
	}
}
