// Package auth is the credential vault worker. It resolves provider API tokens
// for the rest of the harness, registering fn.auth.get_token.
//
// This implementation reads tokens from the process environment. It is the
// swappable layer a secrets-manager worker would replace: any worker that
// registers the same subject and speaks types.TokenRequest/TokenResponse can
// take its place without the rest of the stack changing.
package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

// providerEnv maps a provider name to the environment variable holding its token.
var providerEnv = map[string]string{
	"anthropic": "ANTHROPIC_API_KEY",
	"openai":    "OPENAI_API_KEY",
	"google":    "GEMINI_API_KEY",
	"gemini":    "GEMINI_API_KEY",
}

// Start registers the credential vault on the bus. It is non-blocking: the
// registration lives for the lifetime of the connection.
func Start(ctx context.Context, b *bus.Bus) error {
	_, err := b.Register(bus.SubjAuthGetToken, func(ctx context.Context, data []byte) (any, error) {
		var req types.TokenRequest
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, fmt.Errorf("unmarshal token request: %w", err)
		}

		envVar, ok := providerEnv[req.Provider]
		if !ok {
			return nil, fmt.Errorf("no credential configured for provider %q", req.Provider)
		}
		token := os.Getenv(envVar)
		if token == "" {
			return nil, fmt.Errorf("no credential configured for provider %q", req.Provider)
		}
		return types.TokenResponse{Token: token}, nil
	})
	return err
}
