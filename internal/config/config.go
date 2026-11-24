package config

import (
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/mihudec/fortimetrics/internal/httpoptions"
	"gopkg.in/yaml.v3"
)

type Config struct {
	BatchSize      int      `yaml:"batch_size"`
	FlushInterval  int      `yaml:"flush_interval"`
	Targets        []Target `yaml:"targets"`
	Writers        []Writer `yaml:"writers"`
	QueueSize      int      `yaml:"queue_size"`
	PublishWorkers int      `yaml:"publish_workers"`
}
type Target struct {
	MaxSessions       int               `yaml:"max_sessions"`
	ScrapeJitter      *float64          `yaml:"scrape_jitter"`
	CAFile            string            `yaml:"ca_file"`
	Proxy             *string           `yaml:"proxy"`
	APIKey            string            `yaml:"api_key"`
	APIKeyEnv         string            `yaml:"api_key_env"`
	Type              string            `yaml:"type"`
	Hostname          string            `yaml:"hostname"`
	BaseURL           string            `yaml:"base_url"`
	Username          string            `yaml:"username"`
	Password          string            `yaml:"password"`
	PasswordEnv       string            `yaml:"password_env"`
	VerifySSL         *bool             `yaml:"verify_ssl"`
	VDOM              string            `yaml:"vdom"`
	ScrapeInterval    int               `yaml:"scrape_interval"`
	Timeout           int               `yaml:"timeout"`
	RequestsPerSecond float64           `yaml:"requests_per_second"`
	Burst             int               `yaml:"burst"`
	VirtualServers    []string          `yaml:"virtual_servers"`
	Tags              map[string]string `yaml:"tags"`
	ScrapeConfigs     []Scrape          `yaml:"scrape_configs"`
	Patterns          []*regexp.Regexp  `yaml:"-"`
}
type Scrape struct {
	Topic string            `yaml:"topic"`
	Tags  map[string]string `yaml:"tags"`
}
type Writer struct {
	CAFile    string  `yaml:"ca_file"`
	Proxy     *string `yaml:"proxy"`
	VerifySSL *bool   `yaml:"verify_ssl"`
	Type      string  `yaml:"type"`
	URL       string  `yaml:"url"`
	Method    string  `yaml:"method"`
	Timeout   int     `yaml:"timeout"`
}

func validURL(s string) bool {
	u, e := url.Parse(s)
	return e == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != "" && u.User == nil && u.Fragment == ""
}
func Load(r io.Reader) (Config, error) {
	var wrapper struct {
		Config *Config `yaml:"config"`
	}
	d := yaml.NewDecoder(r)
	d.KnownFields(true)
	if err := d.Decode(&wrapper); err != nil {
		return Config{}, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return Config{}, fmt.Errorf("expected one YAML document")
	}
	if wrapper.Config == nil {
		return Config{}, fmt.Errorf("required config mapping is missing")
	}
	c := *wrapper.Config
	if c.BatchSize == 0 {
		c.BatchSize = 500
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = 1
	}
	if c.BatchSize < 1 || c.FlushInterval < 1 || int64(c.FlushInterval) > int64(time.Duration(1<<63-1)/time.Second) {
		return c, fmt.Errorf("batch_size and flush_interval must be positive and flush_interval must fit a duration")
	}
	if c.QueueSize == 0 {
		c.QueueSize = 1024
	}
	if c.PublishWorkers == 0 {
		c.PublishWorkers = 2
	}
	if c.QueueSize < 1 || c.PublishWorkers < 1 {
		return c, fmt.Errorf("queue_size and publish_workers must be positive")
	}
	if len(c.Targets) == 0 || len(c.Writers) == 0 {
		return c, fmt.Errorf("targets and writers are required")
	}
	for i := range c.Targets {
		t := &c.Targets[i]
		if _, err := httpoptions.Options(t.CAFile, t.Proxy, t.VerifySSL); err != nil {
			return c, fmt.Errorf("target %d: %w", i, err)
		}
		if t.Type == "" {
			t.Type = "fortiadc"
		}
		if t.VDOM == "" {
			t.VDOM = "root"
		}
		if t.ScrapeInterval == 0 {
			t.ScrapeInterval = 30
		}
		if t.MaxSessions == 0 {
			t.MaxSessions = 10
		}
		if t.ScrapeJitter == nil {
			v := 0.1
			t.ScrapeJitter = &v
		}
		if t.MaxSessions < 1 || math.IsNaN(*t.ScrapeJitter) || *t.ScrapeJitter < 0 || *t.ScrapeJitter >= 1 {
			return c, fmt.Errorf("target %d: max_sessions must be positive and scrape_jitter must be in [0,1)", i)
		}
		if t.ScrapeInterval > int((time.Duration(1<<63-1)/time.Second)/10) {
			return c, fmt.Errorf("target %d: scrape_interval too large", i)
		}
		if t.Timeout == 0 {
			t.Timeout = 15
		}
		if t.RequestsPerSecond == 0 {
			t.RequestsPerSecond = 5
		}
		if t.Burst == 0 {
			t.Burst = 1
		}
		if (t.APIKey != "" || t.APIKeyEnv != "") && (t.Username != "" || t.Password != "" || t.PasswordEnv != "") {
			return c, fmt.Errorf("target %d: choose either api_key/api_key_env or username/password credentials", i)
		}
		if t.APIKeyEnv != "" {
			var ok bool
			t.APIKey, ok = os.LookupEnv(t.APIKeyEnv)
			if !ok || t.APIKey == "" {
				return c, fmt.Errorf("target %d: API key environment variable is unset or empty", i)
			}
		}
		if strings.ContainsAny(t.APIKey, " \t\r\n") {
			return c, fmt.Errorf("target %d: API key must not contain whitespace", i)
		}
		if t.PasswordEnv != "" {
			var ok bool
			t.Password, ok = os.LookupEnv(t.PasswordEnv)
			if !ok {
				return c, fmt.Errorf("target %d: password environment variable is unset", i)
			}
		}
		if t.Type != "fortiadc" || t.Hostname == "" || !validURL(t.BaseURL) || (t.APIKey == "" && (t.Username == "" || t.Password == "")) {
			return c, fmt.Errorf("target %d: requires fortiadc type, hostname, HTTP(S) base_url and credentials", i)
		}
		if t.ScrapeInterval < 1 || t.Timeout < 1 || int64(t.ScrapeInterval) > int64(time.Duration(1<<63-1)/time.Second) || int64(t.Timeout) > int64(time.Duration(1<<63-1)/time.Second) || t.RequestsPerSecond <= 0 || math.IsInf(t.RequestsPerSecond, 0) || math.IsNaN(t.RequestsPerSecond) || t.Burst < 1 {
			return c, fmt.Errorf("target %d: invalid interval, timeout or rate limit", i)
		}
		for _, p := range t.VirtualServers {
			re, e := regexp.Compile("^(?:" + p + ")")
			if e != nil {
				return c, fmt.Errorf("target %d: invalid virtual server expression: %w", i, e)
			}
			t.Patterns = append(t.Patterns, re)
		}
		seen := map[string]bool{}
		for _, s := range t.ScrapeConfigs {
			if (s.Topic != "vs_status" && s.Topic != "vs_http_stats") || seen[s.Topic] {
				return c, fmt.Errorf("target %d: unknown or duplicate scrape topic", i)
			}
			seen[s.Topic] = true
		}
	}
	for i := range c.Writers {
		w := &c.Writers[i]
		if w.Type == "stdout" {
			if w.URL != "" || w.Method != "" || w.Timeout != 0 || w.CAFile != "" || w.Proxy != nil || w.VerifySSL != nil {
				return c, fmt.Errorf("writer %d: stdout does not accept HTTP settings", i)
			}
			continue
		}
		if _, err := httpoptions.Options(w.CAFile, w.Proxy, w.VerifySSL); err != nil {
			return c, fmt.Errorf("writer %d: %w", i, err)
		}
		if w.Method == "" {
			w.Method = "POST"
		}
		if w.Timeout == 0 {
			w.Timeout = 15
		}
		if w.Type != "http" || !validURL(w.URL) || w.Method != "POST" || w.Timeout < 1 || int64(w.Timeout) > int64(time.Duration(1<<63-1)/time.Second) {
			return c, fmt.Errorf("writer %d: requires http type, HTTP(S) URL, POST method and positive timeout", i)
		}
	}
	return c, nil
}
func (t Target) Matches(name string) bool {
	if len(t.Patterns) == 0 {
		return true
	}
	for _, p := range t.Patterns {
		if p.MatchString(name) {
			return true
		}
	}
	return false
}
func (t Target) TopicTags(topic string) map[string]string {
	tags := map[string]string{}
	for k, v := range t.Tags {
		tags[k] = v
	}
	for _, s := range t.ScrapeConfigs {
		if s.Topic == topic {
			for k, v := range s.Tags {
				tags[k] = v
			}
		}
	}
	tags["hostname"] = t.Hostname
	tags["vdom"] = t.VDOM
	tags["device_type"] = t.Type
	return tags
}
func (t Target) HTTPStats() bool {
	for _, s := range t.ScrapeConfigs {
		if strings.EqualFold(s.Topic, "vs_http_stats") {
			return true
		}
	}
	return false
}
