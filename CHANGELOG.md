# Changelog

All notable changes to this module are documented here. Versions are git
tags; see "Publishing" in the README.

## Unreleased

## 0.2.1 - 2026-11-03

- Heartbeats omit a shard's `guilds` until that shard is ready, so a restart
  cannot zero the listing's public count.
- Commands are retried every 5 minutes after a `5xx`, a `429` or a failed fetch
  from Discord, until they post or get a terminal `4xx`. A manual `PostCommands`
  no longer waits out a `429` longer than that.
- A `429` on a heartbeat no longer suspends stats and commands, whose limit is
  separate.
- A panicking callback no longer stops a publisher, and `PostX` then returns the
  request's real result instead of `Skipped: "panic"`.
- A panicking `Handler.OnError` no longer crashes the process.
- `WithStatsInterval` is never below 60 seconds; 0 used to spin a core.
- Redirects are no longer followed: one turned a POST into a GET, and its `200`
  read as posted. A `3xx` is now retried like a `5xx`, as in every other SDK.
- The release workflow pins its actions to commit SHAs and refuses a tag that is
  not on `main`.

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
