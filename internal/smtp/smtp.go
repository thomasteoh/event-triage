// Package smtp is the inbound SMTP listener for the event-triage bridge.
// It accepts mail routed from Google Workspace (custom routing → this host on
// TLS port 5225), parses the MIME into a normalized Event (subject/body/from),
// and hands it to the caller. It is a RECEIVER — no mailbox, no storage; the
// message is parsed and published, then gone.
package smtp

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net"
	"net/mail"
	"net/textproto"
	"strings"
	"time"

	"github.com/thomasteoh/event-triage/internal/triage"
)

// Handler is called for each parsed email. Returning an error rejects the
// message (SMTP 550) so the sender's mail server retries or alerts.
type Handler func(context.Context, triage.Event) error

// Server is the TLS SMTP receiver. It accepts one connection at a time and
// handles the SMTP conversation (EHLO/MAIL/RCPT/DATA) manually — no third-party
// SMTP library, only stdlib net/textproto for the envelope and net/mail for
// MIME parsing.
type Server struct {
	addr    string
	cert    string
	key     string
	handler Handler
	logger  *log.Logger
}

// New creates a TLS SMTP server bound to addr. cert/key are CA-signed
// certificate paths (required — Google routing requires TLS + CA cert).
func New(addr, cert, key string, handler Handler, logger *log.Logger) *Server {
	return &Server{addr: addr, cert: cert, key: key, handler: handler, logger: logger}
}

// Listen starts the TLS listener and serves connections until ctx is done.
func (s *Server) Listen(ctx context.Context) error {
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		// Server-side TLS: require the client to present a valid cert chain.
		ClientAuth: tls.RequireAndVerifyClientCert,
	}
	cert, err := tls.LoadX509KeyPair(s.cert, s.key)
	if err != nil {
		return fmt.Errorf("load tls cert: %w", err)
	}
	tlsCfg.Certificates = []tls.Certificate{cert}

	ln, err := tls.Listen("tcp", s.addr, tlsCfg)
	if err != nil {
		return fmt.Errorf("tls listen: %w", err)
	}
	defer ln.Close()

	s.logger.Printf("smtp: listening on %s (TLS)", s.addr)
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			s.logger.Printf("smtp: accept: %v", err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(60 * time.Second))

	tp := textproto.NewConn(conn)
	if err := tp.PrintfLine("220 support.harmonicr.com ESMTP ready"); err != nil {
		s.logger.Printf("smtp: banner: %v", err)
		return
	}

	var from, rcpt string
	var data []byte
	// Minimal SMTP state machine: EHLO → MAIL FROM → RCPT TO → DATA.
	for {
		line, err := tp.ReadLine()
		if err != nil {
			s.logger.Printf("smtp: read: %v", err)
			return
		}
		cmd := strings.TrimSpace(line)
		upper := strings.ToUpper(cmd)
		switch {
		case strings.HasPrefix(upper, "EHLO") || strings.HasPrefix(upper, "HELO"):
			if err := tp.PrintfLine("250-support.harmonicr.com"); err != nil {
				return
			}
			if err := tp.PrintfLine("250 PIPELINING"); err != nil {
				return
			}
		case strings.HasPrefix(upper, "MAIL FROM:"):
			from = strings.TrimSpace(strings.TrimPrefix(cmd[strings.Index(cmd, "FROM:")+len("FROM:"):], "<>"))
			_ = tp.PrintfLine("250 OK")
		case strings.HasPrefix(upper, "RCPT TO:"):
			rcpt = strings.TrimSpace(strings.TrimPrefix(cmd[strings.Index(cmd, "TO:")+len("TO:"):], "<>"))
			_ = tp.PrintfLine("250 OK")
		case strings.HasPrefix(upper, "DATA"):
			_ = tp.PrintfLine("354 End data with <CR><LF>.<CR><LF>")
			body, err := readData(tp)
			if err != nil {
				_ = tp.PrintfLine("451 read error")
				return
			}
			data = body
			_ = tp.PrintfLine("250 OK")
		case upper == "QUIT":
			_ = tp.PrintfLine("221 Bye")
			return
		default:
			// Unknown command: reject but keep the connection open.
			_ = tp.PrintfLine("500 Unknown command")
		}
		if len(data) > 0 {
			break
		}
	}

	if from == "" || len(data) == 0 {
		_ = tp.PrintfLine("550 rejected")
		return
	}
	e, err := parseMail(from, rcpt, data)
	if err != nil {
		s.logger.Printf("smtp: parse mail: %v", err)
		_ = tp.PrintfLine("550 rejected")
		return
	}
	if err := s.handler(context.Background(), e); err != nil {
		s.logger.Printf("smtp: handler: %v", err)
		_ = tp.PrintfLine("550 rejected")
		return
	}
	_ = tp.PrintfLine("250 accepted")
}

// readData reads the DATA section (dot-terminated) from the SMTP conversation.
func readData(tp *textproto.Conn) ([]byte, error) {
	var buf strings.Builder
	for {
		line, err := tp.ReadLine()
		if err != nil {
			return nil, err
		}
		if line == "." {
			break
		}
		// Dot-unstuffing: a lone dot is the terminator; leading double dot
		// is the escaped single dot.
		if strings.HasPrefix(line, "..") {
			line = line[1:]
		}
		buf.WriteString(line)
		buf.WriteString("\r\n")
	}
	return []byte(buf.String()), nil
}

// parseMail parses the raw DATA into a normalized event.
func parseMail(from, rcpt string, data []byte) (triage.Event, error) {
	msg, err := mail.ReadMessage(bytes.NewReader(data))
	if err != nil {
		return triage.Event{}, fmt.Errorf("read message: %w", err)
	}
	header := msg.Header
	subject := header.Get("Subject")
	if subject == "" {
		subject = "(no subject)"
	}
	body, err := readBody(msg)
	if err != nil {
		return triage.Event{}, fmt.Errorf("read body: %w", err)
	}
	return triage.Event{
		Source:  triage.SourceEmail,
		Type:    "support_email",
		Title:   decodeMIMEHeader(subject),
		Detail:  body,
		From:    from,
		Email:   from,
		Company: "",
	}, nil
}

func readBody(msg *mail.Message) (string, error) {
	// Prefer the text/plain part; fall back to the whole body.
	body, err := io.ReadAll(msg.Body)
	if err != nil {
		return "", err
	}
	ct := msg.Header.Get("Content-Type")
	if ct == "" {
		return decodeMIMEHeader(string(body)), nil
	}
	mediatype, params, _ := mime.ParseMediaType(ct)
	if strings.HasPrefix(mediatype, "text/plain") {
		return decodeMIMEHeader(string(body)), nil
	}
	if strings.HasPrefix(mediatype, "multipart/") {
		// Parse multipart into a plain-text summary.
		mr := multipart.NewReader(bytes.NewReader(body), params["boundary"])
		var parts []string
		for {
			p, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return "", err
			}
			b, _ := io.ReadAll(p)
			ct := p.Header.Get("Content-Type")
			mt, _, _ := mime.ParseMediaType(ct)
			if strings.HasPrefix(mt, "text/plain") {
				parts = append(parts, decodeMIMEHeader(string(b)))
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, "\n\n"), nil
		}
		return decodeMIMEHeader(string(body)), nil
	}
	return decodeMIMEHeader(string(body)), nil
}

// decodeMIMEHeader decodes RFC 2047 encoded-words (Subject/From/body) to UTF-8.
func decodeMIMEHeader(s string) string {
	if s == "" {
		return ""
	}
	dec := new(mime.WordDecoder)
	if out, err := dec.DecodeHeader(s); err == nil {
		return out
	}
	return s
}
