package pipeline

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
	httpclient "github.com/mihudec/gohttp/client"
)

func sampleMetric(n int64) metric.Metric {
	return metric.Metric{Measurement: "test", Fields: map[string]any{"value": n}, Timestamp: time.Unix(1, 0)}
}
func TestBatchSizeAndFinalFlush(t *testing.T) {
	in := make(chan metric.Metric, 7)
	out := make(chan []metric.Metric, 3)
	for i := int64(0); i < 7; i++ {
		in <- sampleMetric(i)
	}
	close(in)
	if e := batchMetrics(context.Background(), in, out, 3, time.Hour); e != nil {
		t.Fatal(e)
	}
	if len(out) != 3 {
		t.Fatalf("got %d batches", len(out))
	}
	next := int64(0)
	for _, want := range []int{3, 3, 1} {
		b := <-out
		if len(b) != want {
			t.Fatal("wrong batch size")
		}
		for _, m := range b {
			if m.Fields["value"] != next {
				t.Fatal("metric lost, duplicated, or batch backing array reused")
			}
			next++
		}
	}
}
func TestBatchDeadlineAndNoEmptyPosts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan metric.Metric)
	out := make(chan []metric.Metric)
	done := make(chan error, 1)
	go func() { done <- batchMetrics(ctx, in, out, 500, 20*time.Millisecond) }()
	in <- sampleMetric(1)
	select {
	case b := <-out:
		if len(b) != 1 {
			t.Fatal("wrong batch")
		}
	case <-ctx.Done():
		t.Fatal("partial batch not flushed")
	}
	close(in)
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-out:
		t.Fatal("empty batch emitted")
	case <-ctx.Done():
		t.Fatal("batcher did not finish")
	}
}
func TestBatchCancellationWhileBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan metric.Metric, 1)
	in <- sampleMetric(1)
	done := make(chan error, 1)
	go func() { done <- batchMetrics(ctx, in, make(chan []metric.Metric), 1, time.Hour) }()
	cancel()
	select {
	case e := <-done:
		if e != context.Canceled {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("batcher remained blocked")
	}
}
func TestPublisherSendsMultilineAndSkipsInvalidMetric(t *testing.T) {
	received := make(chan string, 1)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		received <- string(b)
		w.WriteHeader(204)
	}))
	defer s.Close()
	h, e := httpclient.New()
	if e != nil {
		t.Fatal(e)
	}
	p := publisher{cfg: config.Writer{URL: s.URL}, client: h}
	e = p.write(context.Background(), []metric.Metric{sampleMetric(1), {}, sampleMetric(2)})
	if e == nil {
		t.Fatal("invalid metric was not reported")
	}
	body := <-received
	if body != "test value=1i 1000000000\ntest value=2i 1000000000\n" {
		t.Fatal(body)
	}
	if e = p.write(context.Background(), nil); e != nil {
		t.Fatal(e)
	}
	if e = p.write(context.Background(), []metric.Metric{{}}); e == nil {
		t.Fatal("invalid-only batch not rejected")
	}
	select {
	case b := <-received:
		t.Fatal("unexpected empty POST", strings.TrimSpace(b))
	default:
	}
}

func TestStdoutBatch(t *testing.T) {
	var output strings.Builder
	p := publisher{stdout: &output}
	if err := p.write(context.Background(), []metric.Metric{sampleMetric(1), sampleMetric(2)}); err != nil {
		t.Fatal(err)
	}
	if output.String() != "test value=1i 1000000000\ntest value=2i 1000000000\n" {
		t.Fatal(output.String())
	}
}
