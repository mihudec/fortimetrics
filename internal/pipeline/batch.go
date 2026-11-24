package pipeline

import (
	"context"
	"time"

	"github.com/mihudec/fortimetrics/internal/metric"
)

// batchMetrics owns each batch until it hands it to one publisher worker.
// The timer measures age from the first metric, not time since the latest one.
// An unbuffered output keeps pending batches bounded and propagates backpressure.
func batchMetrics(ctx context.Context, in <-chan metric.Metric, out chan<- []metric.Metric, size int, interval time.Duration) error {
	var batch []metric.Metric
	timer := time.NewTimer(interval)
	timer.Stop()
	defer timer.Stop()
	var tick <-chan time.Time
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		select {
		case out <- batch:
			batch = nil
			timer.Stop()
			tick = nil
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for {
		// Ensure continuous input cannot defer the flush deadline.
		if tick != nil {
			select {
			case <-tick:
				if e := flush(); e != nil {
					return e
				}
			default:
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m, ok := <-in:
			if !ok {
				return flush()
			}
			if len(batch) == 0 {
				timer.Reset(interval)
				tick = timer.C
			}
			batch = append(batch, m)
			if len(batch) >= size {
				if e := flush(); e != nil {
					return e
				}
			}
		case <-tick:
			if e := flush(); e != nil {
				return e
			}
		}
	}
}
