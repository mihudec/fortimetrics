package fortiadc

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
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
	c.log.Debugw("discovered real-server members", "members", len(members))
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
	tags["ip"] = node.Address
	if node.AddressType == "ipv6" {
		tags["ip"] = node.Address6
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

// scrapeMembers polls serially. next is shared across sweeps so the pacing also
// applies when a small set of members is polled in consecutive sweeps.
func (c *Client) scrapeMembers(ctx context.Context, members []poolMember, next *time.Time, emit func(metric.Metric) error, report func(error)) error {
	for _, member := range members {
		if wait := time.Until(*next); wait > 0 && !sleep(ctx, wait) {
			return ctx.Err()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		m, err := c.collectMember(ctx, member)
		*next = time.Now().Add(memberPollDelay)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			report(err)
		}
		if len(m.Fields) > 0 {
			if err := emit(m); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Client) pollMembers(ctx context.Context, topic config.Scrape, interval, refresh time.Duration, jitter float64, emit func(metric.Metric) error, report func(error)) {
	if !sleep(ctx, time.Duration(rand.Float64()*float64(interval))) {
		return
	}
	var members []poolMember
	var next, refreshAt time.Time
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
		if err := c.scrapeMembers(ctx, members, &next, emit, report); err != nil {
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
