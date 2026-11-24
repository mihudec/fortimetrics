package fortiadc

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mihudec/fortimetrics/internal/config"
	"github.com/mihudec/fortimetrics/internal/metric"
)

func TestHealthAndJitter(t *testing.T) {
	for status, want := range map[string]int64{"HEALTHY": 0, "DISABLE": 1, "MAINTAINED": 2, "NO_HEALTH_CHECK": 3, "UNHEALTHY": 4, "DEAD": 5, "unknown": 5, "": 5} {
		if health(status) != want {
			t.Fatal(status)
		}
	}
	for _, j := range []float64{0, 0.1, 0.9} {
		for _, r := range []float64{0, 0.5, 1} {
			d := jitterDelay(time.Second, j, r)
			if d < time.Duration(float64(time.Second)*(1-j)) || d > time.Duration(float64(time.Second)*(1+j)) {
				t.Fatal(d)
			}
		}
	}
}
func TestIndependentPollingAndDiscovery(t *testing.T) {
	var discoveries, fast atomic.Int32
	slowStarted := make(chan struct{})
	slowStopped := make(chan struct{})
	updated := make(chan struct{}, 1)
	var startOnce, stopOnce sync.Once
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/load_balance_virtual_server" {
			if name := r.URL.Query().Get("mkey"); name != "" {
				status := "HEALTHY"
				if name == "fast" && fast.Load() >= 2 {
					status = "DEAD"
				}
				fmt.Fprintf(w, `{"payload":{"mkey":%q,"availability":%q}}`, name, status)
				return
			}
			n := discoveries.Add(1)
			if n == 1 {
				io.WriteString(w, `{"payload":[{"mkey":"slow","availability":"HEALTHY"},{"mkey":"fast","availability":"HEALTHY"}]}`)
			} else {
				io.WriteString(w, `{"payload":[{"mkey":"fast","availability":"HEALTHY"},{"mkey":"new","availability":"HEALTHY"}]}`)
			}
			return
		}
		if r.URL.Query().Get("vsname") == "slow" {
			startOnce.Do(func() { close(slowStarted) })
			<-r.Context().Done()
			stopOnce.Do(func() { close(slowStopped) })
			return
		}
		if r.URL.Query().Get("vsname") == "fast" {
			fast.Add(1)
		}
		io.WriteString(w, `{"payload":{"current_sessions":1}}`)
	}))
	defer s.Close()
	c, e := New(config.Target{BaseURL: s.URL, APIKey: "test", Timeout: 2, MaxSessions: 3, RequestsPerSecond: 10000, Burst: 100})
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.poll(ctx, 5*time.Millisecond, 100*time.Millisecond, 0, func(m metric.Metric) error {
			if m.Tags["virtualServerName"] == "fast" && m.Fields["health"] == int64(5) {
				select {
				case updated <- struct{}{}:
				default:
				}
			}
			return nil
		}, func(e error) { t.Errorf("poll failed: %v", e) })
	}()
	select {
	case <-slowStarted:
	case <-ctx.Done():
		t.Fatal("slow poll not started")
	}
	select {
	case <-slowStopped:
	case <-ctx.Done():
		t.Fatal("removed VS was not cancelled")
	}
	if fast.Load() < 2 {
		t.Fatal("slow VS blocked fast VS")
	}
	select {
	case <-updated:
	case <-ctx.Done():
		t.Fatal("health was not refreshed")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pollers did not stop")
	}
}
func TestConcurrentRequestsShareRefreshAndLimit(t *testing.T) {
	var logins, active, peak atomic.Int32
	stale := make(chan struct{})
	var staleCount atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/user/login" {
			fmt.Fprintf(w, `{"token":"t%d"}`, logins.Add(1))
			return
		}
		if r.URL.Path == "/api/load_balance_virtual_server" {
			io.WriteString(w, `{"payload":[{"mkey":"a","availability":"HEALTHY"},{"mkey":"b","availability":"HEALTHY"},{"mkey":"c","availability":"HEALTHY"},{"mkey":"d","availability":"HEALTHY"}]}`)
			return
		}
		n := active.Add(1)
		defer active.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		if r.Header.Get("Authorization") == "Bearer t1" {
			if staleCount.Add(1) == 2 {
				close(stale)
			}
			select {
			case <-stale:
			case <-r.Context().Done():
				return
			}
			w.WriteHeader(401)
			return
		}
		io.WriteString(w, `{"payload":{"current_sessions":1}}`)
	}))
	defer s.Close()
	c, e := New(config.Target{BaseURL: s.URL, Username: "test", Password: "test", Timeout: 2, MaxSessions: 2, RequestsPerSecond: 10000, Burst: 100})
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	count := 0
	if e = c.Collect(ctx, func(metric.Metric) error { count++; return nil }); e != nil {
		t.Fatal(e)
	}
	if count != 4 || peak.Load() != 2 || logins.Load() != 2 {
		t.Fatalf("metrics=%d peak=%d logins=%d", count, peak.Load(), logins.Load())
	}
}
