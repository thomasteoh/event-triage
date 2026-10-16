package smtp

import (
	"strings"
	"testing"

	"github.com/thomasteoh/event-triage/internal/triage"
)

func TestParseMailPlainText(t *testing.T) {
	raw := "From: sender@example.com\r\n" +
		"To: support@harmonicr.com\r\n" +
		"Subject: Need help with pipeline\r\n" +
		"Content-Type: text/plain; charset=UTF-8\r\n" +
		"\r\n" +
		"Please fix the Databricks job.\r\n"
	e, err := parseMail("sender@example.com", "support@harmonicr.com", []byte(raw))
	if err != nil {
		t.Fatalf("parseMail: %v", err)
	}
	if e.Source != triage.SourceEmail {
		t.Errorf("source: want email, got %s", e.Source)
	}
	if e.Title != "Need help with pipeline" {
		t.Errorf("title: %q", e.Title)
	}
	if !strings.Contains(e.Detail, "Please fix the Databricks job.") {
		t.Errorf("detail missing body: %q", e.Detail)
	}
	if e.From != "sender@example.com" {
		t.Errorf("from: %q", e.From)
	}
}

func TestParseMailMIMEEncodedSubject(t *testing.T) {
	// RFC 2047 encoded-word subject.
	raw := "From: sender@example.com\r\n" +
		"To: support@harmonicr.com\r\n" +
		"Subject: =?UTF-8?Q?Need_help?=\r\n" +
		"Content-Type: text/plain\r\n" +
		"\r\n" +
		"body\r\n"
	e, err := parseMail("sender@example.com", "support@harmonicr.com", []byte(raw))
	if err != nil {
		t.Fatalf("parseMail: %v", err)
	}
	if e.Title != "Need help" {
		t.Errorf("decoded title: %q", e.Title)
	}
}

func TestParseMailMultipart(t *testing.T) {
	raw := "From: sender@example.com\r\n" +
		"To: support@harmonicr.com\r\n" +
		"Subject: Multipart issue\r\n" +
		"Content-Type: multipart/alternative; boundary=\"BOUNDARY\"\r\n" +
		"\r\n" +
		"--BOUNDARY\r\n" +
		"Content-Type: text/plain\r\n" +
		"\r\n" +
		"Plain text body.\r\n" +
		"--BOUNDARY\r\n" +
		"Content-Type: text/html\r\n" +
		"\r\n" +
		"<p>HTML body</p>\r\n" +
		"--BOUNDARY--\r\n"
	e, err := parseMail("sender@example.com", "support@harmonicr.com", []byte(raw))
	if err != nil {
		t.Fatalf("parseMail: %v", err)
	}
	if !strings.Contains(e.Detail, "Plain text body.") {
		t.Errorf("multipart detail missing text part: %q", e.Detail)
	}
}

func TestParseMailNoSubject(t *testing.T) {
	raw := "From: sender@example.com\r\n" +
		"To: support@harmonicr.com\r\n" +
		"Content-Type: text/plain\r\n" +
		"\r\n" +
		"body\r\n"
	e, err := parseMail("sender@example.com", "support@harmonicr.com", []byte(raw))
	if err != nil {
		t.Fatalf("parseMail: %v", err)
	}
	if e.Title != "(no subject)" {
		t.Errorf("no-subject title: %q", e.Title)
	}
}
