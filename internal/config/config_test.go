package config

import (
	"strings"
	"testing"
)

func TestRejectInvalidConfig(t *testing.T) {
	for _, s := range []string{"", "targets: []", "config: {targets: [], writers: []}", "config: {unknown: 1}", "config: {}\n---\nconfig: {}"} {
		if _, e := Load(strings.NewReader(s)); e == nil {
			t.Errorf("accepted %q", s)
		}
	}
}

func TestAuthenticationOptions(t *testing.T) {
	t.Setenv("FORTIMETRICS_TEST_KEY", "from-env")
	t.Setenv("FORTIMETRICS_EMPTY_KEY", "")
	for _, tc := range []struct {
		name, auth, key string
		valid           bool
	}{
		{"login", "username: user\n      password: pass", "", true},
		{"literal", "api_key: literal", "literal", true},
		{"environment", "api_key_env: FORTIMETRICS_TEST_KEY", "from-env", true},
		{"override", "api_key: literal\n      api_key_env: FORTIMETRICS_TEST_KEY", "from-env", true},
		{"empty environment", "api_key_env: FORTIMETRICS_EMPTY_KEY", "", false},
		{"missing credentials", "username: user", "", false},
		{"mixed", "api_key: literal\n      username: user\n      password: pass", "", false},
		{"mixed environment", "api_key_env: FORTIMETRICS_TEST_KEY\n      password_env: UNUSED_PASSWORD", "", false},
		{"invalid key", "api_key: 'bad key'", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, e := Load(strings.NewReader("config:\n  targets:\n    - hostname: adc\n      base_url: https://example.test\n      " + tc.auth + "\n  writers:\n    - type: http\n      url: http://localhost/write\n"))
			if (e == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, e)
			}
			if tc.valid && c.Targets[0].APIKey != tc.key {
				t.Fatal("incorrect resolved key")
			}
		})
	}
}

func TestPollingSettings(t *testing.T) {
	for _, tc := range []struct {
		settings string
		valid    bool
	}{
		{"", true}, {"max_sessions: 2\n      scrape_jitter: 0", true},
		{"max_sessions: -1", false}, {"scrape_jitter: -0.1", false}, {"scrape_jitter: 1", false}, {"scrape_jitter: .nan", false}, {"scrape_jitter: .inf", false},
	} {
		c, e := Load(strings.NewReader("config:\n  targets:\n    - hostname: adc\n      base_url: https://example.test\n      api_key: test\n      " + tc.settings + "\n  writers:\n    - type: http\n      url: http://localhost/write\n"))
		if (e == nil) != tc.valid {
			t.Fatalf("settings %q: %v", tc.settings, e)
		}
		if e == nil && tc.settings == "" && (c.Targets[0].MaxSessions != 10 || *c.Targets[0].ScrapeJitter != 0.1) {
			t.Fatal("wrong defaults")
		}
	}
}

func TestBatchSettings(t *testing.T) {
	for _, tc := range []struct {
		settings string
		valid    bool
	}{
		{"", true}, {"batch_size: 100\n  flush_interval: 2", true}, {"batch_size: -1", false}, {"flush_interval: -1", false}, {"flush_interval: 9223372036854775807", false},
	} {
		c, e := Load(strings.NewReader("config:\n  " + tc.settings + "\n  targets:\n    - hostname: adc\n      base_url: https://example.test\n      api_key: test\n  writers:\n    - type: http\n      url: http://localhost/write\n"))
		if (e == nil) != tc.valid {
			t.Fatalf("%q: %v", tc.settings, e)
		}
		if e == nil && tc.settings == "" && (c.BatchSize != 500 || c.FlushInterval != 1) {
			t.Fatal("wrong batch defaults")
		}
	}
}
