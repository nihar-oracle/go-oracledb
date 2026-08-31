/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** rights in the Software, and any and all patent rights owned or freely
** licensable by each licensor hereunder covering either (i) the unmodified
** Software as contributed to or provided by such licensor, or (ii) the Larger
** Works (as defined below), to deal in both
**
** (a) the Software, and
** (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if
** one is included with the Software (each a "Larger Work" to which the Software
** is contributed by such licensors),
**
** without restriction, including without limitation the rights to copy, create
** derivative works of, display, perform, and distribute the Software and make,
** use, sell, offer for sale, import, export, have made, and have sold the
** Software and the Larger Work(s), and to sublicense the foregoing rights on
** either these or other terms.
**
** This license is subject to the following condition:
** The above copyright notice and either this complete permission notice or at
** a minimum a reference to the UPL must be included in all copies or
** substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
** SOFTWARE.
 */

// Package oci provides OCI IAM scoped-access tokens for Oracle Database token
// authentication.
package oci

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/common/auth"
	"github.com/oracle/oci-go-sdk/v65/identitydataplane"
)

const refreshWindow = 5 * time.Minute

// Principal identifies the OCI IAM principal used to obtain scoped database
// access tokens.
type Principal string

const (
	// InstancePrincipal authenticates with the OCI instance principal.
	InstancePrincipal Principal = "instance_principal"
	// ResourcePrincipal authenticates with the OCI resource principal.
	ResourcePrincipal Principal = "resource_principal"
	// OKEWorkloadIdentity authenticates with the OKE workload identity.
	OKEWorkloadIdentity Principal = "oke_workload_identity"
	// ConfigProfile authenticates with a profile in an OCI configuration file.
	ConfigProfile Principal = "config_profile"
)

// Config configures an OCI IAM database token provider.
type Config struct {
	// Principal selects the OCI IAM principal used to obtain database tokens.
	Principal Principal
	// CompartmentOCID and DatabaseOCID derive a database-specific token scope
	// when Scope is empty.
	CompartmentOCID string
	DatabaseOCID    string
	// Scope explicitly selects the scoped-access-token scope. It takes
	// precedence over CompartmentOCID and DatabaseOCID.
	Scope string
	// Region overrides the region reported by the selected principal.
	Region string
	// ConfigFile and ConfigProfile select an OCI configuration profile when
	// Principal is ConfigProfile. An empty ConfigFile uses the OCI SDK default.
	ConfigFile    string
	ConfigProfile string
}

// Provider obtains scoped OCI IAM database tokens and retains the private key
// associated with every returned token until that token expires.
type Provider struct {
	mu          sync.Mutex
	client      scopedTokenClient
	scope       string
	token       string
	expires     time.Time
	generations map[[sha256.Size]byte]tokenGeneration
	refreshing  bool
	refreshDone chan struct{}
	now         func() time.Time
	newKey      func() (*rsa.PrivateKey, error)
}

type scopedTokenClient interface {
	GenerateScopedAccessToken(context.Context, identitydataplane.GenerateScopedAccessTokenRequest) (identitydataplane.GenerateScopedAccessTokenResponse, error)
}

type tokenGeneration struct {
	key     *rsa.PrivateKey
	expires time.Time
}

// dependencies isolates OCI factories plus time and key generation so token
// lifecycle tests remain deterministic and offline.
type dependencies struct {
	now               func() time.Time
	newKey            func() (*rsa.PrivateKey, error)
	newClient         func(common.ConfigurationProvider, string) (scopedTokenClient, error)
	instancePrincipal func() (common.ConfigurationProvider, error)
	resourcePrincipal func() (common.ConfigurationProvider, error)
	workloadIdentity  func() (common.ConfigurationProvider, error)
	configProfile     func(string, string) common.ConfigurationProvider
}

// New constructs a Provider from OCI IAM configuration. The returned value is
// structurally compatible with the Oracle Database driver's signed token
// provider interface without depending on a particular driver release.
func New(config Config) (*Provider, error) {
	return newProvider(config, defaultDependencies())
}

func defaultDependencies() dependencies {
	return dependencies{
		now:               time.Now,
		newKey:            generatePrivateKey,
		newClient:         newDataplaneClient,
		instancePrincipal: instancePrincipalConfigurationProvider,
		resourcePrincipal: resourcePrincipalConfigurationProvider,
		workloadIdentity:  workloadIdentityConfigurationProvider,
		configProfile:     common.CustomProfileConfigProvider,
	}
}

func generatePrivateKey() (*rsa.PrivateKey, error) {
	return rsa.GenerateKey(rand.Reader, 2048)
}

// The OCI factories do not all declare the same return interface. Normalize
// them here so construction and test injection use one function type.
func instancePrincipalConfigurationProvider() (common.ConfigurationProvider, error) {
	return auth.InstancePrincipalConfigurationProvider()
}

func resourcePrincipalConfigurationProvider() (common.ConfigurationProvider, error) {
	return auth.ResourcePrincipalConfigurationProvider()
}

func workloadIdentityConfigurationProvider() (common.ConfigurationProvider, error) {
	return auth.OkeWorkloadIdentityConfigurationProvider()
}

func newProvider(config Config, deps dependencies) (*Provider, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	baseProvider, err := configurationProvider(config, deps)
	if err != nil {
		return nil, err
	}
	if baseProvider == nil {
		return nil, fmt.Errorf("create OCI configuration provider: provider is nil")
	}
	client, err := deps.newClient(baseProvider, config.Region)
	if err != nil {
		return nil, fmt.Errorf("create OCI identity data plane client: %w", err)
	}
	if client == nil {
		return nil, fmt.Errorf("create OCI identity data plane client: client is nil")
	}
	return &Provider{
		client: client,
		scope:  config.Scope,
		now:    deps.now,
		newKey: deps.newKey,
	}, nil
}

func (config Config) normalized() (Config, error) {
	config.Principal = Principal(strings.ToLower(strings.TrimSpace(string(config.Principal))))
	config.CompartmentOCID = strings.TrimSpace(config.CompartmentOCID)
	config.DatabaseOCID = strings.TrimSpace(config.DatabaseOCID)
	config.Scope = strings.TrimSpace(config.Scope)
	config.Region = strings.TrimSpace(config.Region)
	config.ConfigFile = strings.TrimSpace(config.ConfigFile)
	config.ConfigProfile = strings.TrimSpace(config.ConfigProfile)

	switch config.Principal {
	case InstancePrincipal, ResourcePrincipal, OKEWorkloadIdentity, ConfigProfile:
	case "":
		return Config{}, fmt.Errorf("OCI IAM database token provider requires Principal")
	default:
		return Config{}, fmt.Errorf("unsupported OCI IAM principal %q", config.Principal)
	}
	if config.Scope == "" {
		if config.CompartmentOCID == "" {
			return Config{}, fmt.Errorf("OCI IAM database token provider requires CompartmentOCID when Scope is not set")
		}
		if config.DatabaseOCID == "" {
			return Config{}, fmt.Errorf("OCI IAM database token provider requires DatabaseOCID when Scope is not set")
		}
		config.Scope = fmt.Sprintf("urn:oracle:db::id::%s::%s", config.CompartmentOCID, config.DatabaseOCID)
	}
	if config.Principal == ConfigProfile && config.ConfigProfile == "" {
		return Config{}, fmt.Errorf("OCI IAM config_profile principal requires ConfigProfile")
	}
	return config, nil
}

func configurationProvider(config Config, deps dependencies) (common.ConfigurationProvider, error) {
	switch config.Principal {
	case InstancePrincipal:
		provider, err := deps.instancePrincipal()
		if err != nil {
			return nil, fmt.Errorf("create OCI instance principal configuration provider: %w", err)
		}
		return provider, nil
	case ResourcePrincipal:
		provider, err := deps.resourcePrincipal()
		if err != nil {
			return nil, fmt.Errorf("create OCI resource principal configuration provider: %w", err)
		}
		return provider, nil
	case OKEWorkloadIdentity:
		provider, err := deps.workloadIdentity()
		if err != nil {
			return nil, fmt.Errorf("create OCI OKE workload identity configuration provider: %w", err)
		}
		return provider, nil
	case ConfigProfile:
		return deps.configProfile(config.ConfigFile, config.ConfigProfile), nil
	default:
		return nil, fmt.Errorf("unsupported OCI IAM principal %q", config.Principal)
	}
}

func newDataplaneClient(provider common.ConfigurationProvider, region string) (scopedTokenClient, error) {
	if region != "" {
		provider = regionalConfigurationProvider{ConfigurationProvider: provider, region: region}
	}
	client, err := identitydataplane.NewDataplaneClientWithConfigurationProvider(provider)
	if err != nil {
		return nil, err
	}
	return &client, nil
}

type regionalConfigurationProvider struct {
	common.ConfigurationProvider
	region string
}

func (provider regionalConfigurationProvider) Region() (string, error) {
	return provider.region, nil
}

func (provider regionalConfigurationProvider) Refreshable() bool {
	refreshable, ok := provider.ConfigurationProvider.(common.RefreshableConfigurationProvider)
	return ok && refreshable.Refreshable()
}

// Token returns a cached, still-fresh token or obtains a replacement token.
func (provider *Provider) Token(ctx context.Context) (string, error) {
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		provider.mu.Lock()
		now := provider.now()
		if provider.token != "" && now.Add(refreshWindow).Before(provider.expires) {
			token := provider.token
			provider.mu.Unlock()
			return token, nil
		}
		provider.pruneExpiredGenerationsLocked(now)
		if provider.refreshing {
			done := provider.refreshDone
			provider.mu.Unlock()
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-done:
				continue
			}
		}
		provider.refreshing = true
		provider.refreshDone = make(chan struct{})
		done := provider.refreshDone
		provider.mu.Unlock()

		token, generation, err := provider.requestToken(ctx, now)

		provider.mu.Lock()
		if err == nil {
			if provider.generations == nil {
				provider.generations = make(map[[sha256.Size]byte]tokenGeneration)
			}
			provider.token = token
			provider.expires = generation.expires
			provider.generations[sha256.Sum256([]byte(token))] = generation
		}
		provider.refreshing = false
		provider.refreshDone = nil
		close(done)
		provider.mu.Unlock()
		if err != nil {
			return "", err
		}
		return token, nil
	}
}

// PrivateKeyForToken returns the PEM-encoded private key retained for token.
// It rejects tokens that were not returned by this Provider or have expired.
func (provider *Provider) PrivateKeyForToken(ctx context.Context, token string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	provider.pruneExpiredGenerationsLocked(provider.now())
	generation, ok := provider.generations[sha256.Sum256([]byte(token))]
	if !ok {
		return nil, fmt.Errorf("no OCI IAM database private key for token")
	}
	return marshalPrivateKey(generation.key)
}

func (provider *Provider) requestToken(ctx context.Context, now time.Time) (string, tokenGeneration, error) {
	var generation tokenGeneration
	if err := ctx.Err(); err != nil {
		return "", generation, err
	}
	key, err := provider.newKey()
	if err != nil {
		return "", generation, fmt.Errorf("generate OCI IAM database token key: %w", err)
	}
	if key == nil {
		return "", generation, fmt.Errorf("generate OCI IAM database token key: key is nil")
	}
	publicDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return "", generation, fmt.Errorf("encode OCI IAM database token public key: %w", err)
	}
	publicPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER})
	if publicPEM == nil {
		return "", generation, fmt.Errorf("encode OCI IAM database token public key")
	}
	response, err := provider.client.GenerateScopedAccessToken(ctx, identitydataplane.GenerateScopedAccessTokenRequest{
		GenerateScopedAccessTokenDetails: identitydataplane.GenerateScopedAccessTokenDetails{
			Scope:     common.String(provider.scope),
			PublicKey: common.String(string(publicPEM)),
		},
	})
	if err != nil {
		return "", generation, fmt.Errorf("get OCI IAM database token: %w", err)
	}
	if response.Token == nil || *response.Token == "" {
		return "", generation, fmt.Errorf("identity data plane returned an empty OCI IAM database token")
	}
	expires, err := jwtExpiration(*response.Token)
	if err != nil {
		return "", generation, fmt.Errorf("read OCI IAM database token expiration: %w", err)
	}
	if !expires.After(now) {
		return "", generation, fmt.Errorf("identity data plane returned an expired OCI IAM database token")
	}
	return *response.Token, tokenGeneration{key: key, expires: expires}, nil
}

func (provider *Provider) pruneExpiredGenerationsLocked(now time.Time) {
	for id, generation := range provider.generations {
		if !generation.expires.After(now) {
			delete(provider.generations, id)
		}
	}
}

func marshalPrivateKey(key *rsa.PrivateKey) ([]byte, error) {
	if key == nil {
		return nil, fmt.Errorf("encode OCI IAM database token private key: key is nil")
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("encode OCI IAM database token private key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

func jwtExpiration(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return time.Time{}, fmt.Errorf("database token is not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("decode JWT payload: %w", err)
	}
	var claims struct {
		ExpiresAt int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return time.Time{}, fmt.Errorf("decode JWT claims: %w", err)
	}
	if claims.ExpiresAt == 0 {
		return time.Time{}, fmt.Errorf("JWT has no exp claim")
	}
	return time.Unix(claims.ExpiresAt, 0), nil
}
