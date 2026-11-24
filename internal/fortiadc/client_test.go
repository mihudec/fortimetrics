package fortiadc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mihudec/fortimetrics/internal/config"
	"github.com/mihudec/fortimetrics/internal/metric"
)

func TestPartialFailureAndIPv6(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/user/login":
			io.WriteString(w, `{"token":"test"}`)
		case "/api/load_balance_virtual_server":
			io.WriteString(w, `{"payload":[{"mkey":"one","addr-type":"ipv6","address6":"2001:db8::1","status":"disable","availability":"DISABLE"}]}`)
		default:
			io.WriteString(w, `{"payload":-1}`)
		}
	}))
	defer s.Close()
	c, e := New(config.Target{BaseURL: s.URL, Timeout: 1, RequestsPerSecond: 1000, Burst: 10})
	if e != nil {
		t.Fatal(e)
	}
	var metrics []metric.Metric
	e = c.Collect(context.Background(), func(m metric.Metric) error { metrics = append(metrics, m); return nil })
	if e == nil || !strings.Contains(e.Error(), "API error -1") {
		t.Fatal(e)
	}
	if len(metrics) != 1 || metrics[0].Tags["ip"] != "2001:db8::1" || len(metrics[0].Fields) != 3 {
		t.Fatalf("unexpected partial metrics: %+v", metrics)
	}
}
func TestLimiterCancellation(t *testing.T) {
	c, e := New(config.Target{BaseURL: "http://127.0.0.1:1", Timeout: 1, RequestsPerSecond: 0.01, Burst: 1})
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if e = c.Collect(ctx, func(metric.Metric) error { return nil }); e == nil {
		t.Fatal("expected cancellation")
	}
	if time.Since(start) > time.Second {
		t.Fatal("cancellation was not prompt")
	}
}

func TestAPIKeySkipsLoginAndReauthentication(t *testing.T) {
	for _, status := range []int{200, 401, 403} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := 0
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Path != "/api/load_balance_virtual_server" {
					t.Errorf("unexpected request %s", r.URL.Path)
				}
				if r.Header.Get("APITOKEN") != "test-api-key" || r.Header.Get("Authorization") != "" || strings.Contains(r.URL.String(), "test-api-key") {
					t.Error("incorrect API key transport")
				}
				w.WriteHeader(status)
				if status == 200 {
					io.WriteString(w, `{"payload":[]}`)
				} else {
					io.WriteString(w, "test-api-key")
				}
			}))
			defer s.Close()
			c, e := New(config.Target{BaseURL: s.URL, APIKey: "test-api-key", Timeout: 1, RequestsPerSecond: 1000, Burst: 10})
			if e != nil {
				t.Fatal(e)
			}
			e = c.Collect(context.Background(), func(metric.Metric) error { return nil })
			if (e == nil) != (status == 200) {
				t.Fatalf("unexpected error: %v", e)
			}
			if e != nil && strings.Contains(e.Error(), "test-api-key") {
				t.Fatal("key leaked in error")
			}
			if calls != 1 {
				t.Fatalf("got %d requests; expected no login or retry", calls)
			}
		})
	}
}

func TestMissingHealthDoesNotReuseDiscovery(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == virtualServerPath {
			if r.URL.Query().Get("mkey") != "server" {
				t.Error("expected a single VS request")
			}
			io.WriteString(w, `{"payload":[{"mkey":"other","availability":"HEALTHY"}]}`)
			return
		}
		io.WriteString(w, `{"payload":{"current_sessions":3}}`)
	}))
	defer s.Close()
	c, err := New(config.Target{BaseURL: s.URL, APIKey: "test", Timeout: 1, RequestsPerSecond: 100, Burst: 2})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	err = c.collectVS(context.Background(), virtualServer{Name: "server", Availability: "HEALTHY"}, func(m metric.Metric) error { fields = m.Fields; return nil })
	if err == nil || fields["current_sessions"] == nil || fields["health"] != nil || fields["availability"] != nil {
		t.Fatalf("fields=%v, err=%v", fields, err)
	}
}

func TestVSHealthFallsBackToFreshList(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("mkey") == "server" {
			io.WriteString(w, `{"payload":{"mkey":"server","status":"enable"}}`)
			return
		}
		io.WriteString(w, `{"payload":[{"mkey":"other","availability":"HEALTHY"},{"mkey":"server","availability":"UNHEALTHY"}]}`)
	}))
	defer s.Close()
	c, err := New(config.Target{BaseURL: s.URL, APIKey: "test", Timeout: 1, RequestsPerSecond: 100, Burst: 2})
	if err != nil {
		t.Fatal(err)
	}
	vs, err := c.readVSHealth(context.Background(), "server")
	if err != nil || vs.Availability != "UNHEALTHY" {
		t.Fatalf("availability=%q, err=%v", vs.Availability, err)
	}
}
