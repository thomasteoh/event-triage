// Package server exposes the bridge's HTTP health/readiness endpoints and an
// OIDC-gated admin surface. The admin surface FAILS CLOSED: if OIDC is not
// configured, admin routes return 403 — they never open.
package server

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// OIDC is the Zitadel integration for the admin surface.
type OIDC struct {
	Issuer       string
	ClientID     string
	ClientSecret string
	Scopes       []string
	AdminRole    string
	RedirectURL  string
}

// Server is the HTTP health/readiness + admin server.
type Server struct {
	addr     string
	health   func() bool
	ready    func() bool
	oidc     *OIDC
	logger   *log.Logger
	mux      *http.ServeMux
	verifier *oidc.IDTokenVerifier
}

// New creates the HTTP server. oidc may be nil (admin surface fails closed).
func New(addr string, health, ready func() bool, oidc *OIDC, logger *log.Logger) *Server {
	s := &Server{
		addr:   addr,
		health: health,
		ready:  ready,
		oidc:   oidc,
		logger: logger,
		mux:    http.NewServeMux(),
	}
	s.register()
	return s
}

// register wires the routes onto the mux. It is called by New (so routes
// exist before Listen, and are testable via the mux) and by Listen.
func (s *Server) register() {
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if s.health == nil || !s.health() {
			http.Error(w, "unhealthy", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"healthy": true})
	})
	s.mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if s.ready == nil || !s.ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ready": true})
	})
	// Admin surface: fail closed. The route is ALWAYS registered; when OIDC
	// is unconfigured (s.oidc nil or verifier failed) adminGuard 403s, so the
	// surface is never accidentally open and never silently 404s.
	s.mux.HandleFunc("/admin", s.adminGuard(s.adminIndex))
}

// Listen starts the HTTP server until ctx is done.
func (s *Server) Listen(ctx context.Context) error {
	if s.oidc != nil && s.oidc.Issuer != "" {
		if err := s.setupOIDC(); err != nil {
			s.logger.Printf("server: oidc setup: %v (admin surface fails closed)", err)
		}
	}
	srv := &http.Server{
		Addr:              s.addr,
		Handler:           s.mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	s.logger.Printf("server: listening on %s", s.addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) setupOIDC() error {
	provider, err := oidc.NewProvider(context.Background(), s.oidc.Issuer)
	if err != nil {
		return err
	}
	verifier := provider.Verifier(&oidc.Config{ClientID: s.oidc.ClientID})
	s.verifier = verifier
	return nil
}

// adminGuard wraps an admin handler with OIDC token verification. When no
// verifier is configured the handler is never reached (fail closed).
func (s *Server) adminGuard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.verifier == nil {
			http.Error(w, "admin disabled", http.StatusForbidden)
			return
		}
		raw := bearerToken(r)
		if raw == "" {
			http.Error(w, "missing token", http.StatusUnauthorized)
			return
		}
		idToken, err := s.verifier.Verify(r.Context(), raw)
		if err != nil {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}
		var claims struct {
			Roles map[string]any `json:"urn:zitadel:iam:org:project:roles"`
		}
		if err := idToken.Claims(&claims); err != nil {
			http.Error(w, "invalid claims", http.StatusUnauthorized)
			return
		}
		if s.oidc.AdminRole == "" || claims.Roles[s.oidc.AdminRole] == nil {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func (s *Server) adminIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"queues": []string{"events.email", "events.pipeline", "events.log", "events.health"},
	})
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && h[:7] == "Bearer " {
		return h[7:]
	}
	return ""
}
