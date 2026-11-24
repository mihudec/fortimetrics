package fortiadc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/mihudec/fortimetrics/internal/config"
	"github.com/mihudec/fortimetrics/internal/deviceauth"
	"github.com/mihudec/fortimetrics/internal/httpoptions"
	"github.com/mihudec/fortimetrics/internal/metric"
	httpclient "github.com/mihudec/gohttp/client"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Client belongs to one target and supports concurrent virtual-server polling.
type Client struct {
	cfg      config.Target
	log      *zap.SugaredLogger
	http     *httpclient.Client
	token    string
	authGate chan struct{}
	slots    chan struct{}
}

func New(t config.Target) (*Client, error) {
	jar, _ := cookiejar.New(nil)
	log := zap.S().Named("fortiadc").With("target", t.Hostname, "vdom", t.VDOM)
	opts := []httpclient.Option{httpclient.WithBaseURL(t.BaseURL), httpclient.WithHTTPClient(&http.Client{Jar: jar, Timeout: time.Duration(t.Timeout) * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}), httpclient.WithRateLimiter(rate.NewLimiter(rate.Limit(t.RequestsPerSecond), t.Burst)), httpclient.WithUserAgent("fortimetrics")}
	transportOpts, err := httpoptions.Options(t.CAFile, t.Proxy, t.VerifySSL)
	if err != nil {
		return nil, err
	}
	opts = append(opts, transportOpts...)
	if t.APIKey != "" {
		auth, err := deviceauth.APIKey("fortiadc", t.APIKey)
		if err != nil {
			return nil, err
		}
		opts = append(opts, httpclient.WithAuthenticator(auth))
	}
	h, e := httpclient.New(append(opts, httpclient.WithLogger(log))...)
	n := t.MaxSessions
	if n <= 0 {
		n = 10
	}
	return &Client{cfg: t, log: log, http: h, authGate: make(chan struct{}, 1), slots: make(chan struct{}, n)}, e
}
func (c *Client) login(ctx context.Context) error {
	r, e := c.http.NewRequest(ctx, "POST", "/api/user/login", nil, map[string]string{"username": c.cfg.Username, "password": c.cfg.Password})
	if e != nil {
		return e
	}
	var response struct {
		Token string `json:"token"`
	}
	_, e = c.http.DoJSON(r, &response)
	if e != nil {
		return safeError("login", e)
	}
	if response.Token == "" {
		return fmt.Errorf("login response has no token")
	}
	c.token = response.Token
	return nil
}
func safeError(op string, e error) error {
	var he *httpclient.HTTPError
	if errors.As(e, &he) {
		return fmt.Errorf("%s: HTTP %d", op, he.Code)
	}
	if errors.Is(e, context.Canceled) {
		return context.Canceled
	}
	return fmt.Errorf("%s: request failed (%T)", op, e)
}
func (c *Client) get(ctx context.Context, path string, q url.Values, out any) error {
	select {
	case c.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-c.slots }()
	token, err := c.sessionToken(ctx, "", false)
	if err != nil {
		return err
	}
	q.Set("vdom", c.cfg.VDOM)
	for attempt := 0; attempt < 2; attempt++ {
		r, e := c.http.NewRequest(ctx, "GET", path, q, nil)
		if e != nil {
			return e
		}
		if c.cfg.APIKey == "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		body, resp, e := c.http.DoRequest(r)
		if c.cfg.APIKey == "" && resp != nil && resp.StatusCode == 401 && attempt == 0 {
			token, e = c.sessionToken(ctx, token, true)
			if e != nil {
				return e
			}
			continue
		}
		if e != nil {
			return safeError(path, e)
		}
		var envelope struct {
			Payload json.RawMessage `json:"payload"`
		}
		if e = json.Unmarshal([]byte(body), &envelope); e != nil {
			return fmt.Errorf("%s: invalid JSON", path)
		}
		if len(envelope.Payload) == 0 || string(envelope.Payload) == "null" {
			return fmt.Errorf("%s: missing payload", path)
		}
		var code int
		if json.Unmarshal(envelope.Payload, &code) == nil && code < 0 {
			return fmt.Errorf("%s: API error %d", path, code)
		}
		d := json.NewDecoder(strings.NewReader(string(envelope.Payload)))
		d.UseNumber()
		if e = d.Decode(out); e != nil {
			return fmt.Errorf("%s: invalid payload", path)
		}
		return nil
	}
	return fmt.Errorf("authentication failed")
}

const virtualServerPath = "/api/load_balance_virtual_server"

type virtualServer struct {
	Name         string `json:"mkey"`
	Address      string `json:"address"`
	Address6     string `json:"address6"`
	AddressType  string `json:"addr-type"`
	Status       string `json:"status"`
	Availability string `json:"availability"`
}

var numericString = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

// FortiADC can send the same counter as an integer, a decimal, or a string.
// Normalize them to floats so the Influx field type stays consistent.
func addNumericFields(fields, data map[string]any) {
	for key, value := range data {
		var number json.Number
		switch v := value.(type) {
		case json.Number:
			number = v
		case string:
			if !numericString.MatchString(v) {
				continue
			}
			number = json.Number(v)
		default:
			continue
		}
		if f, err := number.Float64(); err == nil && !math.IsInf(f, 0) && !math.IsNaN(f) {
			fields[key] = f
		}
	}
}

func (c *Client) sessionToken(ctx context.Context, rejected string, refresh bool) (string, error) {
	if c.cfg.APIKey != "" {
		return "", nil
	}
	select {
	case c.authGate <- struct{}{}:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	defer func() { <-c.authGate }()
	if c.token == "" || (refresh && c.token == rejected) {
		if e := c.login(ctx); e != nil {
			return "", e
		}
	}
	return c.token, nil
}

func health(status string) int64 {
	switch status {
	case "HEALTHY":
		return 0
	case "DISABLE":
		return 1
	case "MAINTAINED":
		return 2
	case "NO_HEALTH_CHECK":
		return 3
	case "UNHEALTHY":
		return 4
	default:
		return 5
	}
}

func (c *Client) discover(ctx context.Context) ([]virtualServer, error) {
	var servers []virtualServer
	if e := c.get(ctx, virtualServerPath, url.Values{}, &servers); e != nil {
		return nil, e
	}
	result := make([]virtualServer, 0, len(servers))
	seen := map[string]bool{}
	for _, vs := range servers {
		if vs.Name != "" && c.cfg.Matches(vs.Name) && !seen[vs.Name] {
			result = append(result, vs)
			seen[vs.Name] = true
		}
	}
	c.log.Debugw("discovered virtual servers", "matched", len(result), "total", len(servers))
	return result, nil
}

// readVSHealth accepts both the single-object response used by the UI and
// firmware that returns a list. Match the name before using the health data.
func (c *Client) readVSHealth(ctx context.Context, name string) (virtualServer, error) {
	var payload json.RawMessage
	if err := c.get(ctx, virtualServerPath, url.Values{"mkey": {name}}, &payload); err != nil {
		return virtualServer{}, err
	}
	var servers []virtualServer
	if len(payload) > 0 && payload[0] == '{' {
		var vs virtualServer
		if err := json.Unmarshal(payload, &vs); err != nil {
			return virtualServer{}, fmt.Errorf("invalid VS health object: %w", err)
		}
		servers = []virtualServer{vs}
	} else if err := json.Unmarshal(payload, &servers); err != nil {
		return virtualServer{}, fmt.Errorf("invalid VS health list: %w", err)
	}
	for _, vs := range servers {
		if vs.Name == name && vs.Availability != "" {
			return vs, nil
		}
	}
	// The list response may include runtime health when the detail object omits it.
	if len(payload) > 0 && payload[0] == '{' {
		servers = nil
		if err := c.get(ctx, virtualServerPath, url.Values{}, &servers); err != nil {
			return virtualServer{}, err
		}
		for _, vs := range servers {
			if vs.Name == name && vs.Availability != "" {
				return vs, nil
			}
		}
	}
	return virtualServer{}, errors.New("VS health response is missing availability for the requested server")
}

func (c *Client) collectVS(ctx context.Context, vs virtualServer, emit func(metric.Metric) error) error {
	var errs []error
	ip := vs.Address
	if vs.AddressType == "ipv6" {
		ip = vs.Address6
	}
	tags := c.cfg.TopicTags("vs_status")
	tags["virtualServerName"] = vs.Name
	tags["ip"] = ip
	fields := map[string]any{}
	var stats map[string]any
	e := c.get(ctx, "/api/status_history/vs_status", url.Values{"vsname": {vs.Name}}, &stats)
	if e != nil {
		errs = append(errs, e)
	} else {
		addNumericFields(fields, stats)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	current, err := c.readVSHealth(ctx, vs.Name)
	if err != nil {
		errs = append(errs, fmt.Errorf("refresh health: %w", err))
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if current.Status != "" {
		vs.Status = current.Status
	}
	if vs.Status != "" {
		fields["status"] = vs.Status
	}
	// Never carry a health field from the discovery snapshot or the stats payload.
	delete(fields, "health")
	delete(fields, "availability")
	if err == nil {
		fields["availability"] = current.Availability
		fields["health"] = float64(health(current.Availability))
	}
	if len(fields) > 0 {
		if e = emit(metric.Metric{Measurement: "virtualServerStatus", Tags: tags, Fields: fields, Timestamp: time.Now()}); e != nil {
			return e
		}
	}
	if !c.cfg.HTTPStats() {
		return errors.Join(errs...)
	}
	fields, err = c.readHTTPStats(ctx, vs.Name)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	tags = c.cfg.TopicTags("vs_http_stats")
	tags["virtualServerName"] = vs.Name
	tags["ip"] = ip
	if e = emit(metric.Metric{Measurement: "virtualServerHttpStats", Tags: tags, Fields: fields, Timestamp: time.Now()}); e != nil {
		return e
	}
	return errors.Join(errs...)
}

func (c *Client) readHTTPStats(ctx context.Context, name string) (map[string]any, error) {
	const path = "/api/fortiview/get_vs_http"
	c.log.Debugw("fetching VS HTTP statistics", "vs", name, "endpoint", path)
	var categories map[string]any
	if err := c.get(ctx, path, url.Values{"vs": {name}}, &categories); err != nil {
		return nil, fmt.Errorf("VS HTTP statistics: %w", err)
	}
	fields := make(map[string]any)
	for i := 0; i < 4; i++ {
		category, _ := categories[fmt.Sprintf("category_%d", i)].(map[string]any)
		addNumericFields(fields, category)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("VS HTTP statistics: %s returned no numeric counters in category_0 through category_3", path)
	}
	c.log.Debugw("collected VS HTTP statistics", "vs", name, "measurement", "virtualServerHttpStats", "fields", len(fields))
	return fields, nil
}
