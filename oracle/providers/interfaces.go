package providers

import "context"

// ProviderRegistrar registers runtime providers used by the connector during
// connection establishment.
//
// RegisterProvider adds provider to the connector so it is available to future
// connection attempts.
type ProviderRegistrar interface {
	RegisterProvider(Provider)
}

// Provider is the marker interface implemented by connector-extensible runtime
// providers.
type Provider interface{}

/*** TOKEN AUTHENTICATION ***/

// TokenAuthenticationProvider returns the token used for token-based database
// authentication flows.
type TokenAuthenticationProvider interface {
	Provider
	// Token returns the token string to authenticate with and any retrieval error.
	Token(context.Context) (string, error)
}

// SignedTokenAuthenticationProvider extends TokenAuthenticationProvider with
// the private key material required for signed token authentication.
type SignedTokenAuthenticationProvider interface {
	TokenAuthenticationProvider
	// PrivateKeyForToken returns the PEM-encoded private key associated with
	// token. Implementations must keep the association valid for every token
	// they return until that token expires.
	PrivateKeyForToken(context.Context, string) ([]byte, error)
}

/*** END OF TOKEN AUTHENTICATION ***/
