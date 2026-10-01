# github.com/TheDiscordList/integration-discordgo

[![Go Reference](https://pkg.go.dev/badge/github.com/TheDiscordList/integration-discordgo.svg)](https://pkg.go.dev/github.com/TheDiscordList/integration-discordgo)

Publish your discordgo bot's server count, shard heartbeats and application
commands to [TheDiscordList](https://thediscordlist.com) automatically, and
verify its inbound webhooks.

No dependencies beyond discordgo itself. The `webhooks` subpackage is standard
library only.

```sh
go get github.com/TheDiscordList/integration-discordgo
```

## Quickstart

```go
package main

import (
	"os"
	"os/signal"

	"github.com/bwmarrin/discordgo"
	thediscordlist "github.com/TheDiscordList/integration-discordgo"
)

func main() {
	session, _ := discordgo.New("Bot " + os.Getenv("DISCORD_TOKEN"))

	poster := thediscordlist.Autopost(session, os.Getenv("TDL_TOKEN"))

	session.Open()
	defer session.Close()
	defer poster.Stop()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
}
```

That is the whole integration for a single-process bot, and it is also the
recommended production setup. It starts three publishers:

| Publisher | What it sends | Cadence |
| --- | --- | --- |
| Stats | `server_count`, `shard_count` | 5s after READY, then every 5 min, debounced (20s) on guild join/leave |
| Heartbeat | per-shard status, latency and guild counts | every ~30s, plus 1s after any shard connects, resumes or drops |
| Commands | your registered global slash commands | once, ~10s after READY |

Your **stats token** is on the listing's manage page, under the API tab. It is
scoped to one listing, so it is the only credential this package needs.

`Autopost` is safe to call before or after `session.Open()`. The bot id is read
from READY, and the server count from `session.State`. With an empty token it
logs a warning and returns an inert `*Poster` — a forgotten environment
variable never takes down a deploy.

A full example, webhooks included, is in [`examples/bot`](examples/bot/main.go).

## Sharding

A discordgo `Session` is one shard (`ShardID` of `ShardCount`). How many
sessions a process holds decides what it can publish on its own:

| Topology | `server_count` comes from | Stats posted by | Heartbeats |
| --- | --- | --- | --- |
| One session, `ShardCount` 1 | `session.State.Guilds` | this process | this process |
| Every shard as a session in one process | the sum over all sessions | this process | this process, one entry per shard |
| Shards split across processes | your `WithServerCount` | the process with cluster 0 | every process, for itself, with `WithCluster` |

**All shards in one process** — pass every session, so the package sees the
whole bot:

```go
sessions := make([]*discordgo.Session, shardCount)
for i := range sessions {
	sessions[i], _ = discordgo.New("Bot " + token)
	sessions[i].ShardID, sessions[i].ShardCount = i, shardCount
}

poster := thediscordlist.AutopostShards(sessions, os.Getenv("TDL_TOKEN"))

for _, s := range sessions {
	s.Open()
}
```

Calling `Autopost` once per session instead would make each poster see one
shard of many and switch stats and heartbeats off (see below).

**Shards split across processes** — discordgo has no cross-process channel to
sum guild counts over, so each process names itself and one of them supplies
the bot-wide total:

```go
poster := thediscordlist.AutopostShards(mySessions, os.Getenv("TDL_TOKEN"),
	thediscordlist.WithCluster(processIndex, processCount),
	thediscordlist.WithServerCount(func(ctx context.Context) (int, error) {
		return redis.SumGuildCounts(ctx) // however your processes share state
	}),
)
```

Only cluster 0 posts stats and commands. Every process heartbeats for
**itself** on purpose: a manager reporting "all fine" for a process whose host
has died is exactly the failure the status page exists to catch.

> **If the cluster count cannot be determined, heartbeats do not start.** The
> API hard-deletes every cluster row whose id is `>= cluster_count` in the
> request, so one process guessing `1` would erase all of its siblings and
> their monitoring history. The heartbeat publisher reports itself disabled
> with reason `cluster-count-unknown` and logs that `WithCluster` is the fix.
> Losing a status page beats losing data.
>
> Likewise, a process holding some of the shards only sees some of the guilds.
> Without `WithServerCount`, stats are disabled with reason
> `server-count-unknown` rather than publishing a wrong number.

## Options

```go
thediscordlist.Autopost(session, token,
	thediscordlist.WithBotID("161660517914509312"), // default: the application id from READY
	thediscordlist.WithBaseURL("https://thediscordlist.com/api"),

	thediscordlist.WithStatsInterval(5*time.Minute),
	thediscordlist.WithHeartbeatInterval(30*time.Second), // retuned from the API's response
	thediscordlist.WithPostEmptyCommands(),               // an empty list CLEARS the listing

	thediscordlist.WithoutStats(),
	thediscordlist.WithoutHeartbeat(),
	thediscordlist.WithoutCommands(),

	thediscordlist.WithCluster(0, 1),
	thediscordlist.WithServerCount(func(ctx context.Context) (int, error) { return 1234, nil }),
	thediscordlist.WithLogger(slog.Default()),
)
```

`WithBotID` also lets the heartbeat start before the gateway connects, so a
booting bot reads *connecting* on its status page rather than down.

There is a hard floor of 60 seconds between two stats POSTs. A join storm, a
mass reconnect and a manual `PostStats` all route through it, so the ceiling is
structurally one request per minute. An unchanged count is not re-sent for 30
minutes.

`WithPostEmptyCommands` is off by default because posting an empty array
**clears** the command list on your listing. Turn it on only if that is what
you mean.

## Events and manual posting

```go
poster := thediscordlist.Autopost(session, token,
	thediscordlist.OnPosted(func(kind thediscordlist.Kind, status int) {}),
	thediscordlist.OnError(func(err *thediscordlist.Error) {}), // err.Kind, err.Status, err.Retryable
	thediscordlist.OnDisabled(func(kind thediscordlist.Kind, status int, reason string) {}),
)

res := poster.PostStats(ctx) // Result{OK, Status, Skipped}
poster.PostCommands(ctx)     // after re-registering commands in-process
poster.PostHeartbeat(ctx)

poster.Stop()
```

Nothing in this package panics into your process. Timer goroutines recover,
`PostX` never returns an error, and without `OnError` or `OnDisabled` those
events are logged as warnings instead. A false `res.OK` with a `res.Skipped` reason
(`"unchanged"`, `"min-gap"`, `"not-elected"`, `"plan"`, `"empty"`, …) means
nothing was sent, which is usually correct rather than a failure.

An explicit `PostX` also re-arms a publisher that disabled itself, so you can
retry after fixing a token or a listing id without restarting.

## Webhooks

```go
import "github.com/TheDiscordList/integration-discordgo/webhooks"
```

The subpackage imports only the standard library, so a webhook-only service
never compiles discordgo.

### `net/http`

```go
http.Handle("/webhooks/tdl", webhooks.NewHandler(os.Getenv("TDL_WEBHOOK_SECRET"),
	func(ctx context.Context, event *webhooks.Event, d webhooks.Delivery) error {
		switch event.Event {
		case webhooks.EventListingUpvoted:
			var vote webhooks.Upvoted
			if err := json.Unmarshal(event.Data, &vote); err != nil {
				return nil
			}
			// …
		}
		return nil // unknown events included
	}))
```

`*webhooks.Handler` is a plain `http.Handler`, so it mounts unchanged on chi,
gorilla/mux and `http.ServeMux`, and through `gin.WrapH` or `echo.WrapHandler`.
It answers `204` when your function returns nil, `500` when it returns an error
or panics (so the delivery is retried), `202` when it overruns the 8-second
budget (the work carries on, `ctx` is not cancelled), and `400` when
verification fails. Set `Tolerance`, `Timeout` and `OnError` on the returned
`*Handler` to tune it.

### Verifying by hand

```go
body, _ := io.ReadAll(r.Body)
event, err := webhooks.Verify(body, r.Header.Get(webhooks.SignatureHeader), secret)
var verr *webhooks.Error
if errors.As(err, &verr) {
	// verr.Code: MISSING_SIGNATURE, MALFORMED_SIGNATURE, STALE_TIMESTAMP,
	// INVALID_SIGNATURE or MALFORMED_BODY
}
```

`body` must be the exact bytes that arrived. Re-encoding a decoded struct with
`json.Marshal` changes key order, escaping and spacing, and can never verify:
the signature covers bytes, not meaning. `VerifyAt` takes an explicit
tolerance and clock.

### Events

| `event.Event` | Fires for | Decode `event.Data` into |
| --- | --- | --- |
| `listing.bumped` | servers, bots | `webhooks.Bumped` |
| `listing.upvoted` | servers, bots | `webhooks.Upvoted` |
| `server.joined` | servers only | `webhooks.ServerJoined` |
| `bot.added` | bots | `webhooks.BotAdded` |
| `bot.stats_reported` | bots | `webhooks.StatsReported` |
| `webhook.ping` | both | `webhooks.Ping` |

Two of them are not what their names suggest. `bot.added` is a **click**
event, dispatched on the install hand-off — Discord never tells anyone whether
the install completed. And `bot.stats_reported` is not an echo of your own
POST: it also fires when the owner edits the listing in the web UI, and from
the heartbeat's internal stats forward. `event.CreatedAt` is enqueue time, not
send time; never use it for freshness.

### Rules for a healthy endpoint

- **Always answer 2xx**, including for `webhook.ping` and for event names you
  do not recognise. The sender disables an endpoint after 10 consecutive failed
  deliveries. Return nil for anything you do not handle.
- A disabled endpoint is probed with a `webhook.ping` every 6 hours for 7 days,
  and **a 2xx to that probe re-enables it**.
- **Register an exact `https://` URL with no redirect.** The sender treats 3xx
  as a failure, so an `http → https` or trailing-slash redirect burns all five
  retries and counts toward the disable streak.
- **Deduplicate on `Delivery.ID`.** It equals `X-TDL-Delivery` and is stable
  across all five retries of one delivery. There is no in-process cache here on
  purpose — it would be wrong as soon as you run two replicas. A manual
  "redeliver" from the UI mints a new id deliberately, so it is not dropped.
- **Rotating the secret has no grace window.** It is a destructive overwrite,
  and deliveries already queued under the old secret become unverifiable.

## Plans, limits and known behaviour

- **Heartbeats need Gold or higher.** On a `403` the heartbeat publisher
  suspends and reprobes every 30 minutes rather than disabling: the plan is
  checked per request, so a bot can gain it mid-run.
- **Rate limits are shared.** The API counts stats and commands against one
  budget keyed by client IP (60/min; heartbeats have their own 120/min). On a
  `429` this package honours `Retry-After` and `X-RateLimit-Reset` with one
  client-wide suspension for stats and commands; heartbeats bypass it — a stale
  number beats a false "down" on your status page. On shared hosting, other
  bots behind the same egress IP count against you.
- **A `401` disables every publisher**: the token is wrong for all of them.
- **`404` never means "bad token".** It means the id is unknown or belongs to a
  server listing. The distinction is deliberate, so probing cannot enumerate.
- **Discord outages do not dent the count.** discordgo drops a guild from
  `State` when it goes unavailable; this package keeps counting it until it
  comes back or Discord says the bot left.
- **`session.StateEnabled` must stay on**, or the guild cache is empty. With
  it off, pass `WithServerCount`; per-shard guild counts are then omitted from
  heartbeats.
- Without `discordgo.IntentsGuilds`, join and leave events never arrive and the
  count only moves on the next READY. The package warns once.
- Shard latency comes from `session.HeartbeatLatency()` and is omitted until
  the first gateway heartbeat has been acknowledged.

## Requirements

- Go 1.23 or newer (CI runs the two latest releases)
- discordgo v0.28.1 or newer, tested through v0.29.x

## Publishing (maintainers)

Go has no package registry to upload to: a version is a semver git tag on this
repository, served by the module proxy and indexed by pkg.go.dev. There are no
secrets to configure; `release.yml` uses the built-in `GITHUB_TOKEN`.

One-time setup:

1. The repository must be public at `github.com/TheDiscordList/integration-discordgo`,
   matching the `module` line in `go.mod` exactly.

Each release:

1. Move the `Unreleased` entries in `CHANGELOG.md` under the new version and
   merge to `main`.
2. Tag `main` and push the tag: `git tag v0.1.0 && git push origin v0.1.0`.
3. `release.yml` runs vet and the tests, creates the GitHub Release with
   generated notes, and runs
   `GOPROXY=proxy.golang.org go list -m github.com/TheDiscordList/integration-discordgo@v0.1.0`
   so pkg.go.dev picks the version up within minutes.

Never move or delete a pushed tag: the checksum database has already recorded
it. To withdraw a bad version, add a `retract` directive to `go.mod` and tag
the next patch release. A v2 or later needs the `/v2` suffix on the module
path.

## License

MIT
