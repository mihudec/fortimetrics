package fortiadc

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/mihudec/fortimetrics/internal/metric"
)

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}
func jitterDelay(interval time.Duration, jitter, random float64) time.Duration {
	return time.Duration(float64(interval) * (1 - jitter + 2*jitter*random))
}

// Collect discovers and polls each VS once concurrently, without startup jitter.
// Emit calls are serialized for callers that do not provide a concurrent callback.
func (c *Client) Collect(ctx context.Context, emit func(metric.Metric) error) error {
	servers, e := c.discover(ctx)
	if e != nil {
		return e
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var emitMu sync.Mutex
	var errs []error
	send := func(m metric.Metric) error { emitMu.Lock(); defer emitMu.Unlock(); return emit(m) }
	for _, vs := range servers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := c.collectVS(ctx, vs, send); e != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("VS %s: %w", vs.Name, e))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if topic, ok := c.cfg.RealServerStats(); ok && ctx.Err() == nil {
		members, err := c.discoverMembers(ctx, topic)
		if err != nil {
			errs = append(errs, err)
		} else {
			var next time.Time
			err := c.scrapeMembers(ctx, members, &next, emit, func(err error) { errs = append(errs, err) })
			if err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

type pollTask struct {
	mu     sync.RWMutex
	vs     virtualServer
	cancel context.CancelFunc
	done   chan struct{}
}

// Poll runs independent, non-overlapping polling loops per VS. Discovery is
// refreshed every ten intervals; a failed refresh retains the last good snapshot.
// Emit and report callbacks must support concurrent calls.
func (c *Client) Poll(ctx context.Context, emit func(metric.Metric) error, report func(error)) {
	interval := time.Duration(c.cfg.ScrapeInterval) * time.Second
	jitter := 0.1
	if c.cfg.ScrapeJitter != nil {
		jitter = *c.cfg.ScrapeJitter
	}
	if topic, ok := c.cfg.RealServerStats(); ok {
		var members sync.WaitGroup
		members.Go(func() {
			c.pollMembers(ctx, topic, interval, 10*interval, jitter, emit, report)
		})
		defer members.Wait()
	}
	c.poll(ctx, interval, 10*interval, jitter, emit, report)
}
func (c *Client) poll(ctx context.Context, interval, refresh time.Duration, jitter float64, emit func(metric.Metric) error, report func(error)) {
	tasks := map[string]*pollTask{}
	defer func() {
		for _, t := range tasks {
			t.cancel()
		}
		for _, t := range tasks {
			<-t.done
		}
	}()
	for ctx.Err() == nil {
		servers, e := c.discover(ctx)
		if e != nil {
			if ctx.Err() == nil {
				report(e)
			}
		} else {
			current := map[string]virtualServer{}
			for _, vs := range servers {
				current[vs.Name] = vs
			}
			for name, t := range tasks {
				if _, ok := current[name]; !ok {
					t.cancel()
					<-t.done
					delete(tasks, name)
				}
			}
			for name, vs := range current {
				if t, ok := tasks[name]; ok {
					t.mu.Lock()
					t.vs = vs
					t.mu.Unlock()
					continue
				}
				taskCtx, cancel := context.WithCancel(ctx)
				t := &pollTask{vs: vs, cancel: cancel, done: make(chan struct{})}
				tasks[name] = t
				go func() {
					defer close(t.done)
					if !sleep(taskCtx, time.Duration(rand.Float64()*float64(interval))) {
						return
					}
					for taskCtx.Err() == nil {
						t.mu.RLock()
						snapshot := t.vs
						t.mu.RUnlock()
						if e := c.collectVS(taskCtx, snapshot, emit); e != nil && taskCtx.Err() == nil {
							report(fmt.Errorf("VS %s: %w", snapshot.Name, e))
						}
						if !sleep(taskCtx, jitterDelay(interval, jitter, rand.Float64())) {
							return
						}
					}
				}()
			}
		}
		if !sleep(ctx, refresh) {
			return
		}
	}
}
