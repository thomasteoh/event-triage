package peppermint

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/thomasteoh/event-triage/internal/triage"
)

func TestCreateTicketSuccess(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method: want POST, got %s", r.Method)
		}
		if r.URL.Path != "/api/v1/ticket/public/create" {
			t.Errorf("path: want /api/v1/ticket/public/create, got %s", r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content-type: %s", r.Header.Get("Content-Type"))
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true,"id":"ticket-123"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "", "event-triage")
	id, err := c.CreateTicket(context.Background(), &triage.Event{
		Source:  triage.SourceEmail,
		Title:   "Need help",
		Detail:  "body",
		From:    "someone@example.com",
		Email:   "someone@example.com",
	})
	if err != nil {
		t.Fatalf("CreateTicket: %v", err)
	}
	if id != "ticket-123" {
		t.Fatalf("want ticket-123, got %s", id)
	}
	if got["title"] != "Need help" {
		t.Errorf("payload title: %v", got["title"])
	}
	if got["createdBy"] != "event-triage" {
		t.Errorf("payload createdBy: %v", got["createdBy"])
	}
}

func TestCreateTicketNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	c := New(srv.URL, "", "event-triage")
	_, err := c.CreateTicket(context.Background(), &triage.Event{
		Source: triage.SourceEmail,
		Title:  "x",
	})
	if err == nil {
		t.Fatal("want error on non-200")
	}
}

func TestCreateTicketSuccessFalse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":false}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "", "event-triage")
	_, err := c.CreateTicket(context.Background(), &triage.Event{
		Source: triage.SourceEmail,
		Title:  "x",
	})
	if err == nil {
		t.Fatal("want error on success=false")
	}
}

func TestReady(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL, "", "event-triage")
	if !c.Ready() {
		t.Fatal("want ready=true")
	}
}
