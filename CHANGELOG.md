# Changelog

All notable changes to this module are documented here. Versions are git
tags; see "Publishing" in the README.

## Unreleased

## 0.2.0 - 2026-10-18

- `webhooks.Verify` and the `Handler` fail closed on an empty secret, with the
  new `MISSING_SECRET` code; the handler answers `500` until a secret is set.
- The webhook `Handler` answers `401` instead of `400` for a missing or invalid
  signature.
- `listing.bump_ready` webhook event: `EventListingBumpReady` and `BumpReady`.
- Commands are forwarded as discordgo decodes them instead of being rebuilt.
- `408` and `425` are retried like `5xx`; every other `4xx` is reported and not
  retried. A `429` is retried once its wait has passed if that comes before the
  next tick, and its suspension is capped at an hour.
- Out-of-band heartbeats on shard transitions wait until at least 10 seconds
  after the previous beat, instead of firing after 1 second.
- `WithHeartbeatInterval` is clamped to 15–60 seconds.
- A pasted `Bearer ` prefix on the stats token is stripped.

## 0.1.0 - 2026-10-16

- `Autopost` and `AutopostShards`: server count, per-shard heartbeats and
  global slash commands, published from discordgo's session state and events.
- `PostStats`, `PostHeartbeat` and `PostCommands` for manual posts, returning a
  `Result` and never panicking; `OnPosted`, `OnError` and `OnDisabled`
  callbacks.
- Status handling shared with every TheDiscordList SDK: 401 disables every
  publisher, 403 suspends heartbeats for 30 minutes, 404/409 disable, 429
  suspends stats and commands, 5xx retries with backoff.
- `webhooks` subpackage: `Verify`, `VerifyAt` and an `http.Handler`, standard
  library only.
