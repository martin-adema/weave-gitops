package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-logr/logr"
	"github.com/weaveworks/weave-gitops/pkg/featureflags"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/oauth2"
	corev1 "k8s.io/api/core/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	LoginOIDC                  string = "oidc"
	LoginUsername              string = "username"
	DefaultOIDCAuthSecretName  string = "oidc-auth"
	FeatureFlagClusterUser     string = "CLUSTER_USER_AUTH"
	FeatureFlagOIDCAuth        string = "OIDC_AUTH"
	FeatureFlagOIDCPassthrough string = "WEAVE_GITOPS_FEATURE_OIDC_AUTH_PASSTHROUGH"
	FeatureFlagSet             string = "true"

	// ClaimUsername is the default claim for getting the user from OIDC for
	// auth
	ClaimUsername string = "email"

	// ClaimGroups is the default claim for getting the groups from OIDC for
	// auth
	ClaimGroups string = "groups"
)

// OIDCConfig is used to configure an AuthServer to interact with
// an OIDC issuer.
type OIDCConfig struct {
	IssuerURL     string
	ClientID      string
	ClientSecret  string
	RedirectURL   string
	TokenDuration time.Duration
	ClaimsConfig  *ClaimsConfig
}

// This is only used if the OIDCConfig doesn't have a TokenDuration set. If
// that is set then it is used for both OIDC cookies and other cookies.
const defaultCookieDuration time.Duration = time.Hour

// AuthConfig is used to configure an AuthServer.
type AuthConfig struct {
	Log                 logr.Logger
	client              *http.Client
	kubernetesClient    ctrlclient.Client
	tokenSignerVerifier TokenSignerVerifier
	OIDCConfig          OIDCConfig
	authMethods         map[AuthMethod]bool
	namespace           string
	adminSecret         string
}

// AuthServer interacts with an OIDC issuer to handle the OAuth2 process flow.
type AuthServer struct {
	AuthConfig
	provider *oidc.Provider
}

// LoginRequest represents the data submitted by client when the auth flow (non-OIDC) is used.
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// UserInfo represents the response returned from the user info handler.
type UserInfo struct {
	Email  string   `json:"email"`
	ID     string   `json:"id"`
	Groups []string `json:"groups"`
}

// NewOIDCConfigFromSecret takes a corev1.Secret and extracts the fields.
//
// The following keys are required in the secret:
//   - issuerURL
//   - clientID
//   - clientSecret
//   - redirectURL
//
// The following keys are optional
// - tokenDuration - defaults to 1 hour.
// - claimUsername - defaults to "email"
// - claimGroups - defaults to "groups"
func NewOIDCConfigFromSecret(secret corev1.Secret) OIDCConfig {
	cfg := OIDCConfig{
		IssuerURL:    string(secret.Data["issuerURL"]),
		ClientID:     string(secret.Data["clientID"]),
		ClientSecret: string(secret.Data["clientSecret"]),
		RedirectURL:  string(secret.Data["redirectURL"]),
	}
	cfg.ClaimsConfig = claimsConfigFromSecret(secret)

	tokenDuration, err := time.ParseDuration(string(secret.Data["tokenDuration"]))
	if err != nil {
		tokenDuration = time.Hour
	}

	cfg.TokenDuration = tokenDuration

	return cfg
}

func claimsConfigFromSecret(secret corev1.Secret) *ClaimsConfig {
	claimUsername, ok := secret.Data["claimUsername"]
	if !ok {
		claimUsername = []byte(ClaimUsername)
	}

	claimGroups, ok := secret.Data["claimGroups"]
	if !ok {
		claimGroups = []byte(ClaimGroups)
	}

	if len(claimUsername) > 0 && len(claimGroups) > 0 {
		return &ClaimsConfig{
			Username: string(claimUsername),
			Groups:   string(claimGroups),
		}
	}

	return nil
}

func NewAuthServerConfig(log logr.Logger, oidcCfg OIDCConfig, kubernetesClient ctrlclient.Client, tsv TokenSignerVerifier, namespace string, authMethods map[AuthMethod]bool, adminSecret string) (AuthConfig, error) {
	if authMethods[OIDC] {
		if _, err := url.Parse(oidcCfg.IssuerURL); err != nil {
			return AuthConfig{}, fmt.Errorf("invalid issuer URL: %w", err)
		}

		if _, err := url.Parse(oidcCfg.RedirectURL); err != nil {
			return AuthConfig{}, fmt.Errorf("invalid redirect URL: %w", err)
		}
	}

	return AuthConfig{
		Log:                 log.WithName("auth-server"),
		client:              http.DefaultClient,
		kubernetesClient:    kubernetesClient,
		tokenSignerVerifier: tsv,
		OIDCConfig:          oidcCfg,
		namespace:           namespace,
		authMethods:         authMethods,
		adminSecret:         adminSecret,
	}, nil
}

// NewAuthServer creates a new AuthServer object.
func NewAuthServer(ctx context.Context, cfg AuthConfig) (*AuthServer, error) {
	if cfg.authMethods[UserAccount] {
		var secret corev1.Secret
		err := cfg.kubernetesClient.Get(ctx, ctrlclient.ObjectKey{
			Namespace: cfg.namespace,
			Name:      cfg.adminSecret,
		}, &secret)

		if err != nil {
			return nil, fmt.Errorf("could not get secret for cluster user, %w", err)
		} else {
			featureflags.Set(FeatureFlagClusterUser, FeatureFlagSet)
		}
	} else {
		featureflags.Set(FeatureFlagClusterUser, "false")
	}

	var provider *oidc.Provider

	if cfg.OIDCConfig.IssuerURL == "" {
		featureflags.Set(FeatureFlagOIDCAuth, "false")
	} else if cfg.authMethods[OIDC] {
		var err error

		provider, err = oidc.NewProvider(ctx, cfg.OIDCConfig.IssuerURL)
		if err != nil {
			return nil, fmt.Errorf("could not create provider: %w", err)
		}
		featureflags.Set(FeatureFlagOIDCAuth, FeatureFlagSet)
	}

	if featureflags.Get(FeatureFlagOIDCAuth) != FeatureFlagSet && featureflags.Get(FeatureFlagClusterUser) != FeatureFlagSet {
		return nil, fmt.Errorf("neither OIDC auth or local auth enabled, can't start")
	}

	return &AuthServer{cfg, provider}, nil
}

// SetRedirectURL is used to set the redirect URL. This is meant to be used
// in unit tests only.
func (s *AuthServer) SetRedirectURL(url string) {
	s.OIDCConfig.RedirectURL = url
}

func (s *AuthServer) oidcEnabled() bool {
	return featureflags.Get(FeatureFlagOIDCAuth) == FeatureFlagSet
}

func (s *AuthServer) oidcPassthroughEnabled() bool {
	return featureflags.Get(FeatureFlagOIDCPassthrough) == FeatureFlagSet
}

func (s *AuthServer) verifier() *oidc.IDTokenVerifier {
	return s.provider.Verifier(&oidc.Config{ClientID: s.OIDCConfig.ClientID})
}

func (s *AuthServer) oauth2Config(scopes []string) *oauth2.Config {
	// Ensure "openid" scope is always present.
	if !contains(scopes, oidc.ScopeOpenID) {
		scopes = append(scopes, oidc.ScopeOpenID)
	}

	// Request "email" scope to get user's email address.
	if !contains(scopes, ScopeEmail) {
		scopes = append(scopes, ScopeEmail)
	}

	// Request "groups" scope to get user's groups.
	if !contains(scopes, ScopeGroups) {
		scopes = append(scopes, ScopeGroups)
	}

	return &oauth2.Config{
		ClientID:     s.OIDCConfig.ClientID,
		ClientSecret: s.OIDCConfig.ClientSecret,
		RedirectURL:  s.OIDCConfig.RedirectURL,
		Endpoint:     s.provider.Endpoint(),
		Scopes:       scopes,
	}
}

func (s *AuthServer) OAuth2Flow() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if !s.oidcEnabled() {
			JSONError(s.Log, rw, "oidc provider not configured", http.StatusBadRequest)
			return
		}

		s.startAuthFlow(rw, r)
	}
}

func (s *AuthServer) Callback() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		var (
			token *oauth2.Token
			state SessionState
		)

		if r.Method != http.MethodGet {
			rw.Header().Add("Allow", "GET")
			rw.WriteHeader(http.StatusMethodNotAllowed)

			return
		}

		ctx := oidc.ClientContext(r.Context(), s.client)

		// Authorization redirect callback from OAuth2 auth flow.
		if errorCode := r.FormValue("error"); errorCode != "" {
			s.Log.Info("authz redirect callback failed", "error", errorCode, "error_description", r.FormValue("error_description"))
			rw.WriteHeader(http.StatusBadRequest)

			return
		}

		code := r.FormValue("code")
		if code == "" {
			s.Log.Info("code value was empty")
			rw.WriteHeader(http.StatusBadRequest)

			return
		}

		cookie, err := r.Cookie(StateCookieName)
		if err != nil {
			s.Log.Error(err, "cookie was not found in the request", "cookie", StateCookieName)
			rw.WriteHeader(http.StatusBadRequest)

			return
		}

		if state := r.FormValue("state"); state != cookie.Value {
			s.Log.Info("cookie value does not match state form value")
			rw.WriteHeader(http.StatusBadRequest)

			return
		}

		b, err := base64.StdEncoding.DecodeString(cookie.Value)
		if err != nil {
			s.Log.Error(err, "cannot base64 decode cookie", "cookie", StateCookieName, "cookie_value", cookie.Value)
			rw.WriteHeader(http.StatusBadRequest)

			return
		}

		if err := json.Unmarshal(b, &state); err != nil {
			s.Log.Error(err, "failed to unmarshal state to JSON", "state", string(b))
			rw.WriteHeader(http.StatusBadRequest)

			return
		}

		token, err = s.oauth2Config(nil).Exchange(ctx, code)
		if err != nil {
			s.Log.Error(err, "failed to exchange auth code for token", "code", code)
			rw.WriteHeader(http.StatusInternalServerError)

			return
		}

		rawIDToken, ok := token.Extra("id_token").(string)
		if !ok {
			JSONError(s.Log, rw, "no id_token in token response", http.StatusInternalServerError)
			return
		}

		_, err = s.verifier().Verify(r.Context(), rawIDToken)
		if err != nil {
			JSONError(s.Log, rw, fmt.Sprintf("failed to verify ID token: %v", err), http.StatusInternalServerError)
			return
		}

		// Issue ID token cookie
		http.SetCookie(rw, s.createCookie(IDTokenCookieName, rawIDToken))
		http.SetCookie(rw, s.createCookie(AccessTokenCookieName, token.AccessToken))

		// Clear state cookie
		http.SetCookie(rw, s.clearCookie(StateCookieName))

		http.Redirect(rw, r, state.ReturnURL, http.StatusSeeOther)
	}
}

func (s *AuthServer) SignIn() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			rw.Header().Add("Allow", "POST")
			rw.WriteHeader(http.StatusMethodNotAllowed)

			return
		}

		var loginRequest LoginRequest

		err := json.NewDecoder(r.Body).Decode(&loginRequest)
		if err != nil {
			s.Log.Error(err, "Failed to decode from JSON")
			JSONError(s.Log, rw, "Failed to read request body.", http.StatusBadRequest)

			return
		}

		var hashedSecret corev1.Secret

		if err := s.kubernetesClient.Get(r.Context(), ctrlclient.ObjectKey{
			Name:      s.adminSecret,
			Namespace: s.namespace,
		}, &hashedSecret); err != nil {
			s.Log.Error(err, "Failed to query for the secret")
			JSONError(s.Log, rw, "Please ensure that a password has been set.", http.StatusBadRequest)

			return
		}

		if loginRequest.Username != string(hashedSecret.Data["username"]) {
			s.Log.Info("Wrong username")
			rw.WriteHeader(http.StatusUnauthorized)

			return
		}

		if err := bcrypt.CompareHashAndPassword(hashedSecret.Data["password"], []byte(loginRequest.Password)); err != nil {
			s.Log.Error(err, "Failed to compare hash with password")
			rw.WriteHeader(http.StatusUnauthorized)

			return
		}

		signed, err := s.tokenSignerVerifier.Sign(loginRequest.Username)
		if err != nil {
			s.Log.Error(err, "Failed to create and sign token")
			rw.WriteHeader(http.StatusInternalServerError)

			return
		}

		http.SetCookie(rw, s.createCookie(IDTokenCookieName, signed))
		rw.WriteHeader(http.StatusOK)
	}
}

// UserInfo inspects the cookie and attempts to verify it as an admin token. If successful,
// it returns a UserInfo object with the email set to the admin token subject. Otherwise it
// uses the token to query the OIDC provider's user info endpoint and return a UserInfo object
// back or a 401 status in any other case.
func (s *AuthServer) UserInfo(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		rw.Header().Add("Allow", "GET")
		rw.WriteHeader(http.StatusMethodNotAllowed)

		return
	}

	c, err := findAuthCookie(r)
	if err != nil {
		s.Log.Error(err, "Failed to get cookie from request")
		rw.WriteHeader(http.StatusBadRequest)

		return
	}

	claims, err := s.tokenSignerVerifier.Verify(c.Value)
	if err == nil {
		ui := UserInfo{
			ID:    claims.Subject,
			Email: claims.Subject,
		}
		toJSON(rw, ui, s.Log)

		return
	}

	if !s.oidcEnabled() {
		ui := UserInfo{}
		toJSON(rw, ui, s.Log)

		return
	}

	info, err := s.provider.UserInfo(r.Context(), oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: c.Value,
	}))
	if err != nil {
		s.Log.Error(err, "failed to query userinfo")
		JSONError(s.Log, rw, fmt.Sprintf("failed to query user info endpoint: %v", err), http.StatusUnauthorized)

		return
	}

	userPrincipal, err := s.OIDCConfig.ClaimsConfig.PrincipalFromClaims(info)
	if err != nil {
		s.Log.Error(err, "failed to parse user info")
		JSONError(s.Log, rw, fmt.Sprintf("failed to query user info endpoint: %v", err), http.StatusUnauthorized)

		return
	}

	ui := UserInfo{
		ID:     userPrincipal.ID,
		Email:  userPrincipal.ID,
		Groups: userPrincipal.Groups,
	}

	toJSON(rw, ui, s.Log)
}

func toJSON(rw http.ResponseWriter, ui UserInfo, log logr.Logger) {
	b, err := json.Marshal(ui)
	if err != nil {
		JSONError(log, rw, fmt.Sprintf("failed to marshal to JSON: %v", err), http.StatusInternalServerError)
		return
	}

	_, err = rw.Write(b)
	if err != nil {
		log.Error(err, "Failing to write response")
	}
}

func (s *AuthServer) startAuthFlow(rw http.ResponseWriter, r *http.Request) {
	nonce, err := generateNonce()
	if err != nil {
		JSONError(s.Log, rw, fmt.Sprintf("failed to generate nonce: %v", err), http.StatusInternalServerError)
		return
	}

	returnURL := r.URL.Query().Get("return_url")

	if returnURL == "" {
		returnURL = r.URL.String()
	}

	b, err := json.Marshal(SessionState{
		Nonce:     nonce,
		ReturnURL: returnURL,
	})
	if err != nil {
		JSONError(s.Log, rw, fmt.Sprintf("failed to marshal state to JSON: %v", err), http.StatusInternalServerError)
		return
	}

	state := base64.StdEncoding.EncodeToString(b)

	scopes := []string{ScopeProfile}
	authCodeURL := s.oauth2Config(scopes).AuthCodeURL(state)

	// Issue state cookie
	http.SetCookie(rw, s.createCookie(StateCookieName, state))

	http.Redirect(rw, r, authCodeURL, http.StatusSeeOther)
}

func (s *AuthServer) Logout() http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			s.Log.Info("Only POST requests allowed")
			rw.WriteHeader(http.StatusMethodNotAllowed)

			return
		}

		http.SetCookie(rw, s.clearCookie(IDTokenCookieName))
		http.SetCookie(rw, s.clearCookie(AccessTokenCookieName))
		rw.WriteHeader(http.StatusOK)
	}
}

func (s *AuthServer) createCookie(name, value string) *http.Cookie {
	cookie := &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/",
		Expires:  time.Now().UTC().Add(s.OIDCConfig.TokenDuration),
		HttpOnly: true,
		Secure:   false,
	}

	return cookie
}

func (s *AuthServer) clearCookie(name string) *http.Cookie {
	cookie := &http.Cookie{
		Name:    name,
		Value:   "",
		Path:    "/",
		Expires: time.Unix(0, 0),
	}

	return cookie
}

// SessionState represents the state that needs to be persisted between
// the AuthN request from the Relying Party (RP) to the authorization
// endpoint of the OpenID Provider (OP) and the AuthN response back from
// the OP to the RP's callback URL. This state could be persisted server-side
// in a data store such as Redis but we prefer to operate stateless so we
// store this in a cookie instead. The cookie value and the value of the
// "state" parameter passed in the AuthN request are identical and set to
// the base64-encoded, JSON serialised state.
//
// https://openid.net/specs/openid-connect-core-1_0.html#Overview
// https://auth0.com/docs/configure/attack-protection/state-parameters#alternate-redirect-method
// https://community.auth0.com/t/state-parameter-and-user-redirection/8387/2
type SessionState struct {
	Nonce     string `json:"n"`
	ReturnURL string `json:"return_url"`
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}

	return false
}

func JSONError(log logr.Logger, w http.ResponseWriter, errStr string, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)

	response := struct {
		Message string `json:"message"`
		Code    int    `json:"code"`
	}{Message: errStr, Code: code}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error(err, "failed encoding error message", "message", errStr)
	}
}

// try to retrieve the access token obtained through OIDC first and, if that doesn't exist,
// fall back to the ID token issued by authenticating using the cluster-user-auth Secret. This way,
// users can use both ways to log into weave-gitops.
func findAuthCookie(req *http.Request) (*http.Cookie, error) {
	cookieNames := []string{AccessTokenCookieName, IDTokenCookieName}
	for _, name := range cookieNames {
		c, err := req.Cookie(name)
		if err == nil {
			return c, nil
		}
	}

	return nil, http.ErrNoCookie
}
