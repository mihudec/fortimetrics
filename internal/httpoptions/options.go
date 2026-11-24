// Package httpoptions supplies shared TLS and proxy options for gohttp clients.
package httpoptions

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"

	httpclient "github.com/mihudec/gohttp/client"
)

// Options must be applied after WithHTTPClient. A nil proxy preserves the
// default transport's environment proxy behavior; an empty string forces direct.
func Options(caFile string, proxy *string, verifySSL *bool) ([]httpclient.Option, error) {
	var opts []httpclient.Option
	tlsConfig := &tls.Config{}
	if verifySSL != nil {
		tlsConfig.InsecureSkipVerify = !*verifySSL
	}
	if caFile != "" {
		if tlsConfig.InsecureSkipVerify {
			return nil, fmt.Errorf("ca_file cannot be combined with verify_ssl: false")
		}
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read ca_file: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("load system CA certificates: %w", err)
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file contains no valid PEM certificates")
		}
		tlsConfig.RootCAs = roots
	}
	opts = append(opts, httpclient.WithTLSConfig(tlsConfig))
	if proxy != nil {
		if *proxy != "" {
			u, err := url.Parse(*proxy)
			if err != nil || u.Hostname() == "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
				return nil, fmt.Errorf("proxy must be an absolute HTTP(S) or SOCKS5 URL without path, query or fragment")
			}
			switch u.Scheme {
			case "http", "https", "socks5", "socks5h":
			default:
				return nil, fmt.Errorf("proxy scheme must be http, https, socks5 or socks5h")
			}
		}
		opts = append(opts, httpclient.WithProxy(*proxy))
	}
	return opts, nil
}
