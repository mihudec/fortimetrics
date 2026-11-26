package pipeline

import (
	"context"
	"encoding/pem"
	"fmt"
	"github.com/mihudec/fortimetrics/internal/metric"
	httpclient "github.com/mihudec/gohttp/client"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mihudec/fortimetrics/internal/config"
)

func TestCollectionAndFanout(t *testing.T) {
	for _, tc := range []struct {
		name, vsType         string
		enableHTTP, wantHTTP bool
	}{
		{"disabled", "l7-load-balance", false, false},
		{"layer7", "l7-load-balance", true, true},
		{"layer4", "l4-load-balance", true, false},
		{"unknown-layer", "", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logins, optional atomic.Int32
			device := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/user/login" {
					n := logins.Add(1)
					fmt.Fprintf(w, `{"token":"token%d"}`, n)
					return
				}
				if r.Header.Get("Authorization") == "Bearer token1" {
					w.WriteHeader(401)
					return
				}
				if r.URL.Query().Get("vdom") != "test" {
					t.Error("missing vdom")
				}
				switch r.URL.Path {
				case "/api/load_balance_virtual_server":
					if r.URL.Query().Has("mkey") {
						io.WriteString(w, `{"payload":{"mkey":"test vs","status":"enable","availability":"HEALTHY"}}`)
					} else {
						fmt.Fprintf(w, `{"payload":[{"mkey":"test vs","type":%q,"address":"192.0.2.1","status":"enable","availability":"HEALTHY"}]}`, tc.vsType)
					}
				case "/api/status_history/vs_status":
					if r.URL.Query().Get("vsname") != "test vs" {
						t.Error("wrong VS")
					}
					io.WriteString(w, `{"payload":{"app_response":0,"client_rtt":0.25,"current_sessions":14,"total_sessions":37}}`)
				case "/api/fortiview/get_vs_http":
					if r.URL.Query().Get("vs") != "test vs" {
						t.Error("HTTP statistics requested for the wrong VS")
					}
					optional.Add(1)
					io.WriteString(w, `{"payload":{"category_0":{"requests":"42","label":"ignored"},"category_1":{"succ_response":9},"category_2":null,"category_3":{}}}`)
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer device.Close()
			var mu sync.Mutex
			received := make([][]string, 2)
			urls := make([]string, 2)
			for i := range urls {
				s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					b, _ := io.ReadAll(r.Body)
					mu.Lock()
					received[i] = append(received[i], string(b))
					mu.Unlock()
					w.WriteHeader(204)
				}))
				defer s.Close()
				urls[i] = s.URL
			}
			topic := ""
			if tc.enableHTTP {
				topic = "\n      scrape_configs:\n        - topic: vs_http_stats"
			}
			cfg, e := config.Load(strings.NewReader(fmt.Sprintf(`config:
  targets:
    - hostname: adc
      base_url: %s
      username: test
      password: test
      vdom: test
      requests_per_second: 10000
      burst: 20%s
  writers:
    - type: http
      url: %s
    - type: http
      url: %s
`, device.URL, topic, urls[0], urls[1])))
			if e != nil {
				t.Fatal(e)
			}
			if e = Run(context.Background(), cfg, true); e != nil {
				t.Fatal(e)
			}
			if logins.Load() != 2 {
				t.Fatalf("logins = %d", logins.Load())
			}
			want := 1
			if tc.wantHTTP {
				want = 2
			}
			if int(optional.Load()) != want-1 {
				t.Fatal("optional stats requested incorrectly")
			}
			for _, lines := range received {
				if len(lines) != 1 || strings.Count(strings.Join(lines, ""), "\n") != want {
					t.Fatalf("received %d metrics, want %d", len(lines), want)
				}
				joined := strings.Join(lines, "")
				if tc.wantHTTP && (!strings.Contains(joined, "virtualServerHttpStats,") ||
					!strings.Contains(joined, "requests=42,succ_response=9 ") ||
					strings.Contains(joined, "label=")) {
					t.Fatalf("unexpected HTTP counters: %s", joined)
				}
				if !strings.Contains(joined, `virtualServerName=test\ vs app_response=0,availability="HEALTHY",client_rtt=0.25,current_sessions=14,health=0,`) ||
					!strings.Contains(joined, "total_sessions=37 ") {
					t.Fatalf("bad output %s", joined)
				}
			}
		})
	}
}

func TestPublisherFailureDoesNotRetryOrLeakBody(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(503)
		io.WriteString(w, "private response detail")
	}))
	defer s.Close()
	h, e := httpclient.New()
	if e != nil {
		t.Fatal(e)
	}
	p := publisher{cfg: config.Writer{URL: s.URL}, client: h}
	e = p.write(context.Background(), []metric.Metric{{Measurement: "test", Fields: map[string]any{"value": int64(1)}, Timestamp: time.Now()}})
	if e == nil || strings.Contains(e.Error(), "private") || calls.Load() != 1 {
		t.Fatalf("error %v; calls %d", e, calls.Load())
	}
}

func TestCustomCAForDeviceAndWriter(t *testing.T) {
	device := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/load_balance_virtual_server":
			io.WriteString(w, `{"payload":[{"mkey":"vs","address":"192.0.2.1","status":"enable","availability":"HEALTHY"}]}`)
		case "/api/status_history/vs_status":
			io.WriteString(w, `{"payload":{"current_sessions":1}}`)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer device.Close()
	var received atomic.Int32
	sink := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received.Add(1); w.WriteHeader(204) }))
	defer sink.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	certs := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: device.Certificate().Raw}), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: sink.Certificate().Raw})...)
	if e := os.WriteFile(ca, certs, 0600); e != nil {
		t.Fatal(e)
	}
	direct := ""
	c := config.Config{QueueSize: 4, PublishWorkers: 1, Targets: []config.Target{{Type: "fortiadc", BaseURL: device.URL, APIKey: "test", CAFile: ca, Proxy: &direct, Timeout: 1, RequestsPerSecond: 1000, Burst: 10}}, Writers: []config.Writer{{URL: sink.URL, CAFile: ca, Proxy: &direct, Timeout: 1}}}
	if e := Run(context.Background(), c, true); e != nil {
		t.Fatal(e)
	}
	if received.Load() != 1 {
		t.Fatal("metric was not delivered")
	}
}

func TestRealServerTopicFiltersAndPublishesMemberSnapshots(t *testing.T) {
	var memberCalls atomic.Int32
	device := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("vdom") != "test-vdom" {
			t.Error("unexpected VDOM")
		}
		switch r.URL.Path {
		case "/api/load_balance_virtual_server":
			io.WriteString(w, `{"payload":[{"mkey":"prod-api","availability":"HEALTHY"},{"mkey":"prod-other","availability":"HEALTHY"}]}`)
		case "/api/load_balance_virtual_server/get_trees":
			io.WriteString(w, `{"payload":[
				{"mkey":"prod-api","content-routing":"enable","children":[
					{"mkey":"route","children":[{"mkey":"pool","children":[{"mkey":"1","real_server_id":"shared","current-status":"HEALTHY"}]}]}
				]},
				{"mkey":"prod-direct","content-routing":"disable","children":[
					{"mkey":"pool","children":[{"mkey":"1","real_server_id":"shared","current-status":"UNHEALTHY"}]}
				]},
				{"mkey":"prod-other","children":[{"mkey":"ignored","children":[{"mkey":"1","real_server_id":"other"}]}]},
				{"mkey":"test-api","children":[{"mkey":"ignored","children":[{"mkey":"1","real_server_id":"other"}]}]}]}`)
		case "/api/status_history/vs_status":
			io.WriteString(w, `{"payload":{"current_sessions":2}}`)
		case "/api/status_history/rs_status":
			memberCalls.Add(1)
			vs, route := q.Get("vsname"), q.Get("crname")
			if q.Get("poolname") != "pool" || q.Get("member") != "1" || !q.Has("crname") ||
				!((vs == "prod-api" && route == "route") || (vs == "prod-direct" && route == "")) {
				t.Errorf("unexpected member request: %v", q)
			}
			io.WriteString(w, `{"payload":{"app_response":9,"current_sessions":4}}`)
		default:
			t.Errorf("unexpected API path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer device.Close()
	var mu sync.Mutex
	var output strings.Builder
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		io.Copy(&output, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer sink.Close()
	cfg, err := config.Load(strings.NewReader(fmt.Sprintf(`config:
  targets:
    - hostname: adc
      base_url: %s
      api_key: test
      vdom: test-vdom
      virtual_servers: ['prod-']
      requests_per_second: 100
      burst: 20
      scrape_configs:
        - topic: rs_status
          virtual_servers: ['prod-api$', 'prod-direct$', 'test-api$']
          tags:
            group: backends
  writers:
    - type: http
      url: %s
`, device.URL, sink.URL)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := Run(ctx, cfg, true); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	lines := output.String()
	if memberCalls.Load() != 2 || strings.Count(lines, "realServerStatus,") != 2 ||
		!strings.Contains(lines, `virtualServerName=prod-other availability="HEALTHY",current_sessions=2,`) ||
		!strings.Contains(lines, "contentRoutingName=route,") ||
		!strings.Contains(lines, "contentRoutingName=N/A,") ||
		!strings.Contains(lines, "group=backends,") ||
		!strings.Contains(lines, `app_response=9,availability="HEALTHY",current_sessions=4,health=0 `) ||
		!strings.Contains(lines, `app_response=9,availability="UNHEALTHY",current_sessions=4,health=4 `) {
		t.Fatalf("unexpected output with %d member requests: %s", memberCalls.Load(), lines)
	}
}
