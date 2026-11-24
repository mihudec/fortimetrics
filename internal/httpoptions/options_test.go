package httpoptions

import (
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	httpclient "github.com/mihudec/gohttp/client"
)

func testClient(t *testing.T, ca string, proxy *string) *httpclient.Client {
	t.Helper()
	opts, e := Options(ca, proxy, nil)
	if e != nil {
		t.Fatal(e)
	}
	opts = append([]httpclient.Option{httpclient.WithHTTPClient(&http.Client{Timeout: time.Second}), httpclient.WithRetry(0)}, opts...)
	c, e := httpclient.New(opts...)
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func TestCustomCA(t *testing.T) {
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }))
	defer s.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if e := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}), 0600); e != nil {
		t.Fatal(e)
	}
	direct := ""
	for _, tc := range []struct {
		name, ca, url string
		ok            bool
	}{
		{"untrusted", "", s.URL, false},
		{"trusted", ca, s.URL, true},
		{"hostname mismatch", ca, strings.Replace(s.URL, "127.0.0.1", "localhost", 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testClient(t, tc.ca, &direct)
			r, _ := c.NewRequest(context.Background(), "GET", tc.url, nil, nil)
			_, _, e := c.DoRequest(r)
			if (e == nil) != tc.ok {
				t.Fatalf("success=%v error=%v", tc.ok, e)
			}
		})
	}
}
func TestProxyRouting(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() != "http://device.invalid/api/test" {
			t.Errorf("unexpected proxy URL %s", r.URL)
		}
		if r.Header.Get("Proxy-Authorization") != "Basic dXNlcjpwYXNz" {
			t.Error("missing proxy authentication")
		}
		io.WriteString(w, "proxied")
	}))
	defer proxy.Close()
	proxyURL := strings.Replace(proxy.URL, "http://", "http://user:pass@", 1)
	c := testClient(t, "", &proxyURL)
	r, _ := c.NewRequest(context.Background(), "GET", "http://device.invalid/api/test", nil, nil)
	body, _, e := c.DoRequest(r)
	if e != nil || body != "proxied" {
		t.Fatal(body, e)
	}
}
func TestInvalidOptions(t *testing.T) {
	invalid := filepath.Join(t.TempDir(), "invalid.pem")
	os.WriteFile(invalid, []byte("not a certificate"), 0600)
	for _, path := range []string{invalid, invalid + "missing"} {
		if _, e := Options(path, nil, nil); e == nil {
			t.Fatal("accepted invalid CA file")
		}
	}
	disabled := false
	if _, e := Options(invalid, nil, &disabled); e == nil {
		t.Fatal("accepted CA with disabled verification")
	}
	for _, proxy := range []string{"not-a-url", "ftp://proxy.test", "http://user:secret@proxy.test/path", "http://proxy.test?x=1", "http://proxy.test#fragment"} {
		_, e := Options("", &proxy, nil)
		if e == nil {
			t.Fatal("accepted invalid proxy")
		}
		if strings.Contains(e.Error(), "secret") {
			t.Fatal("proxy password leaked")
		}
	}
	for _, proxy := range []string{"", "http://proxy.test:8080", "https://proxy.test:8443", "socks5://proxy.test:1080", "socks5h://user:pass@proxy.test:1080"} {
		opts, e := Options("", &proxy, nil)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = httpclient.New(opts...); e != nil {
			t.Fatal(e)
		}
	}
}
