package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mihudec/fortimetrics/internal/config"
	"github.com/mihudec/fortimetrics/internal/fortiadc"
	"github.com/mihudec/fortimetrics/internal/httpoptions"
	"github.com/mihudec/fortimetrics/internal/metric"
	httpclient "github.com/mihudec/gohttp/client"
	"go.uber.org/zap"
)

type publisher struct {
	cfg    config.Writer
	client *httpclient.Client
	stdout io.Writer
}

func (p publisher) write(ctx context.Context, batch []metric.Metric) error {
	var body strings.Builder
	var encodingErrors []error
	for _, m := range batch {
		line, e := m.Line()
		if e != nil {
			encodingErrors = append(encodingErrors, e)
			continue
		}
		body.WriteString(line)
		body.WriteByte('\n')
	}
	encodingErr := errors.Join(encodingErrors...)
	if body.Len() == 0 {
		return encodingErr
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(encodingErr, err)
	}
	if p.stdout != nil {
		_, err := io.WriteString(p.stdout, body.String())
		return errors.Join(encodingErr, err)
	}
	req, e := http.NewRequestWithContext(ctx, "POST", p.cfg.URL, strings.NewReader(body.String()))
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	_, resp, e := p.client.DoRequest(req)
	if e != nil {
		if resp != nil {
			return errors.Join(encodingErr, fmt.Errorf("sink HTTP %d", resp.StatusCode))
		}
		return errors.Join(encodingErr, fmt.Errorf("sink request failed (%T)", e))
	}
	return encodingErr
}

// Run bounds memory with a blocking queue. Workers fan each metric out to every
// sink; multiple sinks never compete for (and accidentally split) metrics.
func Run(ctx context.Context, c config.Config, once bool) error {
	log := zap.S()
	clients := make([]*fortiadc.Client, len(c.Targets))
	for i, t := range c.Targets {
		cl, e := fortiadc.New(t)
		if e != nil {
			return e
		}
		clients[i] = cl
	}
	pubs := make([]publisher, len(c.Writers))
	for i, w := range c.Writers {
		if w.Type == "stdout" {
			pubs[i] = publisher{stdout: os.Stdout}
			continue
		}
		transportOpts, e := httpoptions.Options(w.CAFile, w.Proxy, w.VerifySSL)
		if e != nil {
			return e
		}
		opts := []httpclient.Option{httpclient.WithHTTPClient(&http.Client{Timeout: time.Duration(w.Timeout) * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }})}
		opts = append(opts, transportOpts...)
		h, e := httpclient.New(append(opts, httpclient.WithLogger(log.Named("writer").With("writer", i)))...)
		if e != nil {
			return e
		}
		pubs[i] = publisher{cfg: w, client: h}
	}
	queue := make(chan metric.Metric, c.QueueSize)
	publishCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var failed atomic.Bool
	var workers sync.WaitGroup
	batchSize := c.BatchSize
	if batchSize == 0 {
		batchSize = 500
	}
	flushInterval := c.FlushInterval
	if flushInterval == 0 {
		flushInterval = 1
	}
	batches := make(chan []metric.Metric)
	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(batches)
		if e := batchMetrics(publishCtx, queue, batches, batchSize, time.Duration(flushInterval)*time.Second); e != nil {
			failed.Store(true)
			log.Errorw("batching stopped before drain completed", "error", e)
		}
	}()
	for i := 0; i < c.PublishWorkers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for batch := range batches {
				log.Debugw("publishing metric batch", "metrics", len(batch), "writers", len(pubs))
				for j, p := range pubs {
					if e := p.write(publishCtx, batch); e != nil {
						failed.Store(true)
						log.Errorw("publish failed", "writer", j, "error", e)
					}
				}
			}
		}()
	}
	var collectors sync.WaitGroup
	for i, cl := range clients {
		collectors.Add(1)
		go func() {
			defer collectors.Done()
			t := c.Targets[i]
			_, rsEnabled := t.RealServerStats()
			log.Infow("collecting FortiADC metrics", "target", t.Hostname, "vdom", t.VDOM,
				"vs_http_stats", t.HTTPStats(), "rs_status", rsEnabled)
			emit := func(m metric.Metric) error {
				select {
				case queue <- m:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			report := func(e error) {
				if ctx.Err() == nil {
					failed.Store(true)
					log.Errorw("collection failed", "target", t.Hostname, "error", e)
				}
			}
			if once {
				if e := cl.Collect(ctx, emit); e != nil {
					report(e)
				}
			} else {
				cl.Poll(ctx, emit, report)
			}
		}()
	}
	collectors.Wait()
	close(queue)
	// Give pending deliveries a bounded drain period, even after SIGTERM.
	timer := time.AfterFunc(30*time.Second, cancel)
	workers.Wait()
	timer.Stop()
	if failed.Load() {
		return errors.New("one or more collection or publishing operations failed")
	}
	return nil
}
