package deviceauth

import (
	"net/http"
	"testing"
)

func TestDeviceHeaders(t *testing.T) {
	for _, tc := range []struct{ device, header, value string }{{"fortiadc", "APITOKEN", "secret"}, {"fortigate", "Authorization", "Bearer secret"}} {
		t.Run(tc.device, func(t *testing.T) {
			a, e := APIKey(tc.device, "secret")
			if e != nil {
				t.Fatal(e)
			}
			r, _ := http.NewRequest("GET", "https://example.test/api", nil)
			if e = a.Authorize(r); e != nil {
				t.Fatal(e)
			}
			if r.Header.Get(tc.header) != tc.value || len(r.Header) != 1 || r.URL.RawQuery != "" {
				t.Fatal("incorrect authentication headers")
			}
		})
	}
	for _, key := range []string{"", "bad\nkey", "bad key"} {
		if _, e := APIKey("fortiadc", key); e == nil {
			t.Fatal("accepted invalid key")
		}
	}
	if _, e := APIKey("unknown", "secret"); e == nil {
		t.Fatal("accepted unsupported type")
	}
}
