# Changelog

All notable changes to this module are documented here. Versions are git
tags; see "Publishing" in the README.

## Unreleased

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
