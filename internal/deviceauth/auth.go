// Package deviceauth maps device families to their REST API key headers.
package deviceauth

import (
	"fmt"
	httpclient "github.com/mihudec/gohttp/client"
	"strings"
)

func APIKey(deviceType, key string) (httpclient.Authenticator, error) {
	if key == "" || strings.ContainsAny(key, " \t\r\n") {
		return nil, fmt.Errorf("API key must be nonempty and contain no whitespace")
	}
	switch deviceType {
	case "fortiadc":
		return httpclient.APIKeyHeader{Header: "APITOKEN", Value: key}, nil
	case "fortigate":
		return httpclient.BearerToken(key), nil
	default:
		return nil, fmt.Errorf("unsupported API key device type %q", deviceType)
	}
}
