# fortimetrics

Fortimetrics collects FortiADC virtual server health, connection statistics, and optional FortiView HTTP counters. It sends batched Influx line protocol to Telegraf or prints it to stdout. The initial release supports FortiADC targets and reimplements the Python `fadcmetrics` and `fadcclient` collectors in Go.

## Quick start

Download the binary and `SHA256SUMS` from [GitHub Releases](https://github.com/mihudec/fortimetrics/releases). Binaries are available for macOS and Linux on amd64 and arm64. Verify the matching checksum, then make the binary executable with `chmod +x`.

Copy [fortimetrics.example.yml](fortimetrics.example.yml) to `fortimetrics.yml`, set the appliance and writer addresses, and supply credentials through the environment. The example uses `FORTIADC_PASSWORD`.

```sh
./fortimetrics -config fortimetrics.yml -check-config
./fortimetrics -config fortimetrics.yml -once
./fortimetrics -config fortimetrics.yml
```

Replace `./fortimetrics` with the name of your downloaded binary. Use `--version` to see its version, commit, and build date.

`-check-config` does no network I/O. `-once` scrapes all devices, drains the queue, and returns nonzero if collection or delivery failed. SIGINT/SIGTERM cancels collection and allows up to 30 seconds for pending publishing.

The YAML preserves the Python `config.targets`, `writers`, `scrape_configs`, tags, and virtual-server regex structure. Intervals/timeouts use seconds. TLS verification defaults to true. A literal `password` is supported; `password_env` takes precedence. Writers support HTTP POST and stdout. Unknown keys/topics fail validation. Target and topic tags are combined; identity tags (`hostname`, `vdom`, `device_type`, `virtualServerName`, `ip`) are authoritative.

## Authentication

Existing `username` plus `password`/`password_env` configurations continue to work. This mode posts credentials to `/api/user/login` and uses the returned session token; it is not HTTP Basic authentication.

Alternatively, replace those credential fields on a target with:

```yaml
      api_key_env: FORTIADC_API_KEY
      # Or: api_key: your-rest-api-admin-key
```

`api_key_env` overrides a literal `api_key`; an unset or empty environment variable fails validation. API-key credentials cannot be combined with username/password fields. Keys are loaded at startup; restart after rotation.

FortiADC sends the key in the `APITOKEN` header, following [Fortinet's REST API example](https://community.fortinet.com/fortiadc-7/technical-tip-export-system-debug-file-via-rest-api-187196). The shared authentication helper also supports FortiGate's [`Authorization: Bearer` header](https://docs.fortinet.com/document/fortigate/7.4.6/administration-guide/940602). FortiGate collection remains unimplemented, so `type: fortigate` is still rejected by configuration validation. Keys are never put in query parameters. A rejected API key is reported without falling back to password login.

## TLS and proxies

Each target and HTTP writer accepts these optional settings independently:

```yaml
      verify_ssl: true
      ca_file: /etc/fortimetrics/company-ca.pem
      proxy: http://proxy.example.com:8080
```

`ca_file` is a PEM certificate bundle added to the system CA roots. Certificate chain and hostname validation remain enabled. Relative paths resolve from the process working directory. Missing/unreadable files and bundles without valid certificates fail startup and `-check-config`. Combining `ca_file` with `verify_ssl: false` is rejected. Mount the CA file read-only at the configured path when using Docker. Certificates are loaded at startup; restart after changing the bundle.

`proxy` supports `http://`, `https://`, `socks5://`, and `socks5h://` URLs, including optional `user:password@` credentials (URL-encode special characters). An explicit proxy overrides environment settings, including `NO_PROXY`. Use `proxy: ""` to force direct connections. Omitting `proxy` preserves Go's default `HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY` behavior. Device settings apply to both login and metric requests; writer settings apply to publishing. Proxy URLs are validated without including credentials in error messages.

## Collection and delivery

Every device has its own client, credentials, cookie jar, and token-bucket rate limiter, including login and retries. With username/password login, a 401 triggers one reauthentication. API-key authentication skips login and never reauthenticates on a 401/403. Discovery reads `/api/load_balance_virtual_server` at startup and every ten scrape intervals. Each matching VS has its own non-overlapping polling loop reading `/api/status_history/vs_status`. A slow VS does not hold up the next poll of other VSs, subject to the shared rate limit, request concurrency limit, and publishing backpressure. Discovery adds/removes pollers and updates metadata; a failed refresh retains the last good snapshot. Base `virtualServerStatus` metrics contain IP/name tags, configured `status`, health `availability`, and numeric session/traffic/timing fields returned by the status endpoint. If statistics fail, status metadata is still emitted and the scrape reports an error. Missing statistics are never fabricated as zero.

`vs_http_stats` explicitly enables `/api/fortiview/get_vs_http`; numeric fields from `category_0` through `category_3` become the separate measurement `virtualServerHttpStats`. Counters encoded as numeric strings are accepted as well. Counter names retain the spelling returned by the appliance. `total_sessions` and other appliance values are passed through without assuming they are lifetime counters; no rates are derived.

Collectors send typed `metric.Metric` structs through a bounded, blocking queue. A shared batcher groups up to `batch_size` metrics (default 500), flushing no later than `flush_interval` seconds (default 1) after the first metric arrives, provided a publish worker is available. Publish workers format each batch as newline-separated Influx line protocol with nanosecond timestamps and deliver the batch to every configured writer. HTTP writers use POST. Integers retain their precision. Multiple workers can deliver out of order. Delivery is in-memory, best effort: failed POSTs are logged and their batches discarded, with no automatic POST retries, durable spool, or exactly-once guarantee. One unavailable device does not stop others. A slow sink causes backpressure. The batcher hands batches directly to workers without an extra buffered batch queue. A full queue or busy workers may extend delivery latency beyond the flush interval. Shutdown and `-once` flush the final partial batch within the existing 30-second drain deadline. Empty batches are never posted; invalid metrics are reported and skipped without losing valid metrics in the same batch. Set `batch_size: 1` to restore one metric per POST.

To trace HTTP counter collection, run `fortimetrics -config fortimetrics.yml -once -log-level debug`. Startup logs show whether `vs_http_stats` is enabled for each target. Debug logs show the per-VS requests and how many counters were collected. Request failures and responses without counters are logged with the target and VS. Add a stdout writer to see the resulting Influx lines. Zap logs go to stderr. The default log level is `info`; `warn` and `error` are also supported.

## Polling and health codes

Per-target `max_sessions` defaults to 10 and limits concurrent device requests. `scrape_jitter` defaults to 0.1 and must be in `[0, 1)`. Each VS starts after a random delay between zero and `scrape_interval` seconds, then waits `scrape_interval ± scrape_jitter * scrape_interval` after completing its poll. Setting jitter to zero fixes subsequent intervals but retains the initial stagger, matching Python. All requests, including discovery and authentication, still share the target rate limiter. Token refresh is serialized across pollers.

`-once` discovers and polls every matching VS concurrently exactly once without startup jitter, then drains publishing. Cancellation stops timers, rate-limit waits and requests, and joins all pollers before closing the queue. Metrics may arrive in completion order.

The integer `health` field matches the updated Python collector: `HEALTHY=0`, `DISABLE=1`, `MAINTAINED=2`, `NO_HEALTH_CHECK=3`, `UNHEALTHY=4`, `DEAD=5`; unrecognized status strings also map to 5. Existing string `status` and `availability` fields remain. Each VS poll requests current `availability` using `GET /api/load_balance_virtual_server?mkey=<name>&vdom=<vdom>` through the shared device limiter. If a detail object lacks availability, the collector reads the VS list again for that poll. If health cannot be fetched, the scrape reports an error and omits `availability` and `health` while publishing any available statistics. Real-server collection from the newer Python version is not included.

For debugging, add `- type: stdout` under `config.writers`, or use it as the only writer. Stdout prints one Influx line per metric. Logs go to stderr.

## Telegraf

Configure a Telegraf HTTP listener accepting Influx line protocol:

```toml
[[inputs.http_listener_v2]]
  service_address = ":8186"
  paths = ["/write"]
  methods = ["POST"]
  data_format = "influx"
```

## Docker

Tagged releases publish Linux amd64 and arm64 images to GitHub Container Registry. Authenticate to `ghcr.io` with an account that has package access, then run:

```sh
docker run --rm -e FORTIADC_PASSWORD \
  -v "$PWD/fortimetrics.yml:/etc/fortimetrics/config.yml:ro" \
  ghcr.io/mihudec/fortimetrics:0.1.0
```

Pass your API-key environment variable instead if the target uses one. Use a Telegraf address reachable from the container. The container runs as UID 65532, which needs read access to the mounted YAML and any CA bundle.

## Build and test

Build from a checkout with Go 1.27.4. The [gohttp snapshot](third_party/gohttp/README.md) is included for reproducible builds.

```sh
make build VERSION=0.1.0    # dist/{macos,linux}/{amd64,arm64}/fortimetrics
make test                  # Go tests
make check                 # go vet and race tests
make clean                 # Remove dist/
docker build -t fortimetrics:local .
```

CI runs `make check` and builds and smoke-tests the container. Pushing an unprefixed version tag such as `0.1.0` builds the release binaries and checksums, publishes the multi-platform image under the full version and major/minor tags, and attaches the binaries to the GitHub release. Local tests use simulated FortiADC and Telegraf servers.
