package httprecoverymiddleware

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHelloHandler verifies the /hello route returns 200 with body "hello".
func TestHelloHandler(t *testing.T) {
	var log bytes.Buffer
	srv := NewServer(&log)

	req := httptest.NewRequest(http.MethodGet, "/hello", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("/hello status = %d, want 200", rec.Code)
	}
	body := strings.TrimSpace(rec.Body.String())
	if body != "hello" {
		t.Fatalf("/hello body = %q, want \"hello\"", body)
	}
}

// TestPanicHandlerReturns500 verifies that a panicking handler returns HTTP 500.
func TestPanicHandlerReturns500(t *testing.T) {
	var log bytes.Buffer
	srv := NewServer(&log)

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("/panic status = %d, want 500", rec.Code)
	}
}

// TestServerContinuesAfterPanic verifies the server keeps working after a panic.
func TestServerContinuesAfterPanic(t *testing.T) {
	var log bytes.Buffer
	srv := NewServer(&log)

	// First request panics.
	req1 := httptest.NewRequest(http.MethodGet, "/panic", nil)
	rec1 := httptest.NewRecorder()
	srv.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusInternalServerError {
		t.Fatalf("first /panic status = %d, want 500", rec1.Code)
	}

	// Second request must work normally — server must not be dead.
	req2 := httptest.NewRequest(http.MethodGet, "/hello", nil)
	rec2 := httptest.NewRecorder()
	srv.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("/hello after panic status = %d, want 200", rec2.Code)
	}
}

// TestStackTraceLogged verifies the stack trace is written to the log writer.
func TestStackTraceLogged(t *testing.T) {
	var log bytes.Buffer
	srv := NewServer(&log)

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	logged := log.String()
	if logged == "" {
		t.Fatal("expected stack trace in log, got empty string")
	}
	// runtime/debug.Stack() output always contains "goroutine"
	if !strings.Contains(logged, "goroutine") && !strings.Contains(logged, "panic") {
		t.Fatalf("log does not look like a stack trace: %q", logged)
	}
}

// TestRecoveryMiddlewareDirectly tests RecoveryMiddleware with a custom panicking handler.
func TestRecoveryMiddlewareDirectly(t *testing.T) {
	var log bytes.Buffer
	panicHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("custom panic value")
	})
	wrapped := RecoveryMiddleware(&log, panicHandler)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(log.String(), "custom panic value") {
		t.Fatalf("log does not contain panic value: %q", log.String())
	}
}

// TestRecoveryMiddlewareNoopOnCleanHandler verifies middleware is transparent when no panic.
func TestRecoveryMiddlewareNoopOnCleanHandler(t *testing.T) {
	cleanHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		io.WriteString(w, "ok")
	})
	wrapped := RecoveryMiddleware(io.Discard, cleanHandler)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Fatalf("body = %q, want \"ok\"", rec.Body.String())
	}
}

// TestConcurrentRequests fires many requests concurrently to catch races.
func TestConcurrentRequests(t *testing.T) {
	var log bytes.Buffer
	srv := NewServer(&log)

	done := make(chan struct{}, 40)
	for i := 0; i < 20; i++ {
		go func() {
			req := httptest.NewRequest(http.MethodGet, "/hello", nil)
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, req)
			done <- struct{}{}
		}()
		go func() {
			req := httptest.NewRequest(http.MethodGet, "/panic", nil)
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, req)
			done <- struct{}{}
		}()
	}
	for i := 0; i < 40; i++ {
		<-done
	}
}
