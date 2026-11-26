package fortiadc

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mihudec/fortimetrics/internal/config"
	"github.com/mihudec/fortimetrics/internal/metric"
)

const memberPollDelay = time.Second

type topologyNode struct {
	Key            string         `json:"mkey"`
	ContentRouting string         `json:"content-routing"`
	ScheduleList   string         `json:"schedule-list"`
	RealServer     string         `json:"real_server_id"`
	Address        string         `json:"address"`
	Address6       string         `json:"address6"`
	AddressType    string         `json:"addr-type"`
	Port           string         `json:"port"`
	Status         string         `json:"status"`
	Availability   string         `json:"current-status"`
	Children       []topologyNode `json:"children"`
}

type memberPath struct {
	vs, route, pool, id string
}

type poolMember struct {
	path memberPath
	node topologyNode
}

func (c *Client) discoverMembers(ctx context.Context, topic config.Scrape) ([]poolMember, error) {
	var servers []topologyNode
	if err := c.get(ctx, virtualServerPath+"/get_trees", url.Values{}, &servers); err != nil {
		return nil, fmt.Errorf("RS discovery: %w", err)
	}
	var members []poolMember
	seen := make(map[memberPath]bool)
	selectedVS := make(map[string]bool)
	addPool := func(vs, route string, pool topologyNode) {
		if pool.Key == "" {
			return
		}
		for _, node := range pool.Children {
			path := memberPath{vs: vs, route: route, pool: pool.Key, id: node.Key}
			if node.Key == "" || node.RealServer == "" || seen[path] {
				continue
			}
			seen[path] = true
			selectedVS[vs] = true
			members = append(members, poolMember{path: path, node: node})
		}
	}

	for _, vs := range servers {
		if vs.Key == "" || !c.cfg.Matches(vs.Key) || !topic.Matches(vs.Key) {
			continue
		}
		if vs.ScheduleList == "enable" {
			c.log.Warnw("skipping scheduled topology for RS metrics", "vs", vs.Key)
			continue
		}
		if vs.ContentRouting == "enable" {
			for _, route := range vs.Children {
				if route.ScheduleList == "enable" {
					c.log.Warnw("skipping scheduled topology for RS metrics", "vs", vs.Key, "route", route.Key)
					continue
				}
				if route.Key == "" {
					continue
				}
				for _, pool := range route.Children {
					addPool(vs.Key, route.Key, pool)
				}
			}
		} else {
			for _, pool := range vs.Children {
				addPool(vs.Key, "", pool)
			}
		}
	}
	names := make([]string, 0, len(selectedVS))
	for name := range selectedVS {
		names = append(names, name)
	}
	slices.Sort(names)
	c.log.Infow("discovered real-server members", "members", len(members), "virtual_servers", names)
	return members, nil
}

func (c *Client) collectMember(ctx context.Context, member poolMember) (metric.Metric, error) {
	path, node := member.path, member.node
	tags := c.cfg.TopicTags("rs_status")
	tags["virtualServerName"] = path.vs
	tags["contentRoutingName"] = path.route
	if path.route == "" {
		tags["contentRoutingName"] = "N/A"
	}
	tags["realServerPoolName"] = path.pool
	tags["poolMemberId"] = path.id
	tags["realServerName"] = node.RealServer
	tags["realServerIP"] = node.Address
	if node.AddressType == "ipv6" {
		tags["realServerIP"] = node.Address6
	}
	tags["port"] = node.Port

	var stats map[string]any
	err := c.get(ctx, "/api/status_history/rs_status", url.Values{
		"vsname": {path.vs}, "crname": {path.route}, "poolname": {path.pool}, "member": {path.id},
	}, &stats)
	fields := make(map[string]any)
	if err == nil {
		addNumericFields(fields, stats)
	}
	if node.Status != "" {
		fields["status"] = node.Status
	}
	if node.Availability != "" {
		fields["availability"] = node.Availability
		fields["health"] = float64(health(node.Availability))
	}
	if err != nil {
		err = fmt.Errorf("RS %s/%s/%s/%s: %w", path.vs, path.route, path.pool, path.id, err)
	}
	return metric.Metric{Measurement: "realServerStatus", Tags: tags, Fields: fields, Timestamp: time.Now()}, err
}

// Each worker has its own cooldown, retained across sweeps. All worker
// requests also pass through the shared device limits in c.get.
func (c *Client) scrapeMembers(ctx context.Context, members []poolMember, next []time.Time, emit func(metric.Metric) error, report func(error)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		metric metric.Metric
		err    error
	}
	results := make(chan result)
	var cursor atomic.Int64
	var workers sync.WaitGroup
	for i := range min(len(next), len(members)) {
		workers.Go(func() {
			for ctx.Err() == nil {
				index := cursor.Add(1) - 1
				if index >= int64(len(members)) {
					return
				}
				if wait := time.Until(next[i]); wait > 0 && !sleep(ctx, wait) {
					return
				}
				m, err := c.collectMember(ctx, members[index])
				next[i] = time.Now().Add(memberPollDelay)
				select {
				case results <- result{metric: m, err: err}:
				case <-ctx.Done():
					return
				}
			}
		})
	}
	go func() {
		workers.Wait()
		close(results)
	}()

	// Keep callbacks serialized within the member sweep, including in -once.
	var emitErr error
	for r := range results {
		if ctx.Err() != nil {
			continue
		}
		if r.err != nil {
			report(r.err)
		}
		if len(r.metric.Fields) > 0 {
			if err := emit(r.metric); err != nil {
				emitErr = err
				cancel()
			}
		}
	}
	if emitErr != nil {
		return emitErr
	}
	return ctx.Err()
}

func (c *Client) pollMembers(ctx context.Context, topic config.Scrape, interval, refresh time.Duration, jitter float64, emit func(metric.Metric) error, report func(error)) {
	if !sleep(ctx, time.Duration(rand.Float64()*float64(interval))) {
		return
	}
	var members []poolMember
	next := make([]time.Time, topic.SessionLimit())
	var refreshAt time.Time
	for ctx.Err() == nil {
		start := time.Now()
		if !start.Before(refreshAt) {
			discovered, err := c.discoverMembers(ctx, topic)
			if err != nil {
				if ctx.Err() == nil {
					report(err)
				}
			} else {
				members = discovered
			}
			refreshAt = time.Now().Add(refresh)
		}
		if err := c.scrapeMembers(ctx, members, next, emit, report); err != nil {
			if ctx.Err() == nil {
				report(err)
			}
			return
		}
		delay := jitterDelay(interval, jitter, rand.Float64()) - time.Since(start)
		if !sleep(ctx, delay) {
			return
		}
	}
}
