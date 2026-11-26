package fortiadc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/mihudec/fortimetrics/internal/config"
	"github.com/mihudec/fortimetrics/internal/metric"
)

func TestMemberWorkersShareDeviceSlots(t *testing.T) {
	enteredVS := make(chan struct{}, 2)
	enteredRS := make(chan struct{}, 3)
	releaseVS := make(chan struct{}, 1)
	releaseRS := make(chan struct{})
	device := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered, release := enteredVS, (<-chan struct{})(releaseVS)
		if r.URL.Path == "/api/status_history/rs_status" {
			entered, release = enteredRS, releaseRS
		}
		entered <- struct{}{}
		select {
		case <-release:
			io.WriteString(w, `{"payload":{"current_sessions":1}}`)
		case <-r.Context().Done():
		}
	}))
	defer device.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := New(config.Target{
		BaseURL: device.URL, APIKey: "test", Timeout: 5, MaxSessions: 3,
		RequestsPerSecond: 1000, Burst: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	var virtualRequests sync.WaitGroup
	defer func() { cancel(); virtualRequests.Wait() }()
	for range 2 {
		virtualRequests.Go(func() {
			var stats map[string]any
			if err := c.get(ctx, "/api/status_history/vs_status", url.Values{}, &stats); err != nil && ctx.Err() == nil {
				t.Error(err)
			}
		})
	}
	await := func(event <-chan struct{}, name string) {
		t.Helper()
		select {
		case <-event:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", name)
		}
	}
	assertBlocked := func(event <-chan struct{}, reason string) {
		t.Helper()
		select {
		case <-event:
			t.Fatal(reason)
		case <-time.After(25 * time.Millisecond):
		}
	}
	await(enteredVS, "first VS request")
	await(enteredVS, "second VS request")

	members := []poolMember{
		{path: memberPath{vs: "vs", pool: "pool", id: "1"}},
		{path: memberPath{vs: "vs", pool: "pool", id: "2"}},
		{path: memberPath{vs: "vs", pool: "pool", id: "3"}},
	}
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- c.scrapeMembers(ctx, members, make([]time.Time, 2), func(metric.Metric) error { return nil }, func(err error) { t.Error(err) })
	}()
	defer func() {
		cancel()
		<-done
	}()

	await(enteredRS, "first member request")
	assertBlocked(enteredRS, "member request exceeded the shared target limit")
	releaseVS <- struct{}{}
	await(enteredRS, "second member request")
	releaseVS <- struct{}{}
	virtualRequests.Wait()
	assertBlocked(enteredRS, "member request exceeded the topic limit despite free target slots")

	close(releaseRS)
	await(enteredRS, "third member request")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
