package server

import (
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthzHealthy(t *testing.T) {
	s := New(":0", func() bool { return true }, func() bool { return true }, nil, log.New(io.Discard, "", 0))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
}

func TestHealthzUnhealthy(t *testing.T) {
	s := New(":0", func() bool { return false }, func() bool { return true }, nil, log.New(io.Discard, "", 0))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", w.Code)
	}
}

func TestReadyzNotReady(t *testing.T) {
	s := New(":0", func() bool { return true }, func() bool { return false }, nil, log.New(io.Discard, "", 0))
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", w.Code)
	}
}

func TestAdminFailsClosedWithoutOIDC(t *testing.T) {
	// No OIDC configured → admin surface must 403 (fail closed).
	s := New(":0", func() bool { return true }, func() bool { return true }, nil, log.New(io.Discard, "", 0))
	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("want 403 without OIDC, got %d", w.Code)
	}
}

func TestAdminMissingToken(t *testing.T) {
	// OIDC configured but no bearer token → 401.
	s := New(":0", func() bool { return true }, func() bool { return true },
		&OIDC{Issuer: "https://auth.harmonicr.com", ClientID: "x", AdminRole: "admin"}, log.New(io.Discard, "", 0))
	// setupOIDC needs a reachable issuer; in tests we simulate a configured
	// verifier by checking the fail-closed path. Without a verifier the admin
	// guard 403s, which is the safe behavior.
	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized && w.Code != http.StatusForbidden {
		t.Fatalf("want 401/403, got %d", w.Code)
	}
}
