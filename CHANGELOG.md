# Changelog

## 0.2.1

- Added topic-level `max_sessions` for `rs_status`. Real-server workers use slots from the target’s shared concurrency limit and share its request rate limit. The topic defaults to one worker.
- Startup logs now show whether real-server polling is enabled. Discovery logs list the virtual servers selected for VS and real-server polling.
- Renamed the generated `ip` tag to `virtualServerIP` on `virtualServerStatus` and `virtualServerHttpStats`, and to `realServerIP` on `realServerStatus`. Update queries and dashboards that use the old tag for new data.

## 0.2.0

- Added opt-in `rs_status` collection. Fortimetrics discovers pool members from the FortiADC topology and polls each matching member directly, publishing the snapshot and topology health as `realServerStatus`.
- Added a `virtual_servers` regex filter under the `rs_status` topic. It narrows real-server polling within the target’s VS selection. Base VS collection uses the target filter as before.
- Real-server polling runs one member at a time per appliance, with a one-second pause after each poll and the existing device-wide request limits. Direct and content-routed pools are supported. Scheduled pools are skipped.
- `vs_http_stats` now requests HTTP counters only for virtual servers discovered as Layer 7. Other VS types still produce base status metrics.

## 0.1.3

- Write the generated `health` field as an InfluxDB float, preserving the existing health codes.

## 0.1.2

- Write fetched numeric fields as InfluxDB floats for consistent typing across scrapes.
- Allow binary publication to proceed when Docker’s build-cache export fails after a successful image push.

## 0.1.1

- Use the public `gohttp` module at `v0.1.0` in place of the bundled snapshot.

## 0.1.0

- Initial FortiADC collector with VS health and session statistics, optional FortiView HTTP stats, and batched Telegraf or stdout output.
- Added Linux and macOS binaries for amd64 and arm64, plus a multi-platform Linux container image.
