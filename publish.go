package thediscordlist

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/bwmarrin/discordgo"
)

const (
	statsInterval   = 5 * time.Minute
	statsDebounce   = 20 * time.Second
	statsMinGap     = time.Minute
	statsFirstDelay = 5 * time.Second
	statsRemeasure  = time.Minute
	// Republish an unchanged count this often, so a listing never looks abandoned.
	statsForceAfter = 30 * time.Minute

	heartbeatInterval = 30 * time.Second
	heartbeatMin      = 15 * time.Second
	heartbeatMax      = time.Minute
	heartbeatJitter   = 5 * time.Second

	// Offset from the first stats post: both share one server-side rate budget.
	commandsDelay = 10 * time.Second
	// The Gold plan is checked per request, so a bot can gain it mid-run.
	planReprobe    = 30 * time.Minute
	requestTimeout = 10 * time.Second
	maxAttempts    = 3
	maxLatency     = 10 * time.Minute
	maxThrottle    = time.Hour
)

func (p *Poster) scheduleLocked(pub *publisher, d time.Duration, run func(context.Context)) {
	if p.stopped {
		return
	}
	if pub.timer != nil {
		pub.timer.Stop()
	}
	pub.timer = time.AfterFunc(max(d, 0), func() {
		defer func() {
			if r := recover(); r != nil {
				p.cfg.logger.Error("thediscordlist: recovered from a panic", "kind", pub.kind, "panic", r)
			}
		}()
		run(p.ctx)
	})
}

func (p *Poster) disable(pub *publisher, status int, reason string) {
	p.mu.Lock()
	if pub.timer != nil {
		pub.timer.Stop()
	}
	first := pub.disabled == ""
	if first {
		pub.disabled = reason
	}
	p.mu.Unlock()
	if first {
		p.emitDisabled(pub.kind, status, reason)
	}
}

func (p *Poster) startHeartbeatLocked() {
	if p.beating || p.heartbeat == nil || p.heartbeat.disabled != "" || p.botID == "" {
		return
	}
	p.beating = true
	p.scheduleLocked(p.heartbeat, 0, p.tickHeartbeat)
}

func (p *Poster) startAllLocked() {
	if p.started || p.botID == "" {
		return
	}
	p.started = true
	p.startHeartbeatLocked()
	if !p.topo.elected {
		return
	}
	if p.stats != nil && p.stats.disabled == "" {
		p.scheduleLocked(p.stats, p.cfg.statsFirstDelay, p.tickStats)
	}
	if p.commands != nil {
		p.scheduleLocked(p.commands, p.cfg.commandsDelay, func(ctx context.Context) { p.publishCommands(ctx) })
	}
}

// beatSoonLocked sends an out-of-band heartbeat after a shard transition. A
// 30s cadence against a 90s grace window leaves no margin during a reconnect.
// Repeated calls coalesce into one beat.
func (p *Poster) beatSoonLocked() {
	if p.beating && p.heartbeat.disabled == "" {
		p.scheduleLocked(p.heartbeat, time.Second, p.tickHeartbeat)
	}
}

// guildChangedLocked is a trailing debounce, hard-capped at minGap so a join
// storm cannot starve the post. Startup GUILD_CREATEs arrive before the first
// post and are ignored: READY already listed those guilds.
func (p *Poster) guildChangedLocked() {
	if p.stats == nil || !p.started || !p.topo.elected || p.stats.disabled != "" || p.lastPostAt.IsZero() {
		return
	}
	now := time.Now()
	if p.debounceCap.IsZero() {
		p.debounceCap = now.Add(p.cfg.statsMinGap)
	}
	p.scheduleLocked(p.stats, min(p.cfg.statsDebounce, p.debounceCap.Sub(now)), p.tickStats)
}

func (p *Poster) tickStats(ctx context.Context)     { p.publishStats(ctx, false) }
func (p *Poster) tickHeartbeat(ctx context.Context) { p.beat(ctx, false) }

type statsBody struct {
	ServerCount int `json:"server_count"`
	// Always sent: the server stores a missing shard_count as null.
	ShardCount int `json:"shard_count"`
}

func (p *Poster) publishStats(ctx context.Context, manual bool) Result {
	pub := p.stats
	if !pub.sending.TryLock() {
		return Result{Skipped: "in-flight"}
	}
	defer pub.sending.Unlock()

	p.mu.Lock()
	p.debounceCap = time.Time{}
	if !manual && pub.disabled != "" {
		p.mu.Unlock()
		return Result{Skipped: "disabled"}
	}
	now := time.Now()
	if !manual && now.Before(pub.suspendedUntil) {
		p.scheduleLocked(pub, pub.suspendedUntil.Sub(now), p.tickStats)
		p.mu.Unlock()
		return Result{Skipped: "plan"}
	}
	// A hard floor that join storms, reconnects and manual posts all route
	// through, so the ceiling is structurally one request per minGap.
	if next := p.lastPostAt.Add(p.cfg.statsMinGap); !p.lastPostAt.IsZero() && now.Before(next) {
		p.scheduleLocked(pub, next.Sub(now), p.tickStats)
		p.mu.Unlock()
		return Result{Skipped: "min-gap"}
	}
	botID := p.botID
	p.mu.Unlock()

	count, ok := p.measure(ctx)

	p.mu.Lock()
	if !ok {
		p.scheduleLocked(pub, p.cfg.statsRemeasure, p.tickStats)
		p.mu.Unlock()
		return Result{Skipped: "unmeasurable"}
	}
	shardCount := p.topo.shardCount
	// Every stats post re-indexes the listing and fires a webhook even when
	// nothing moved, and the server does not dedupe, so the skip lives here.
	if count == p.lastCount && shardCount == p.lastShardCount && time.Now().Before(p.lastPostAt.Add(statsForceAfter)) {
		p.scheduleLocked(pub, p.cfg.statsInterval, p.tickStats)
		p.mu.Unlock()
		return Result{Skipped: "unchanged"}
	}
	p.lastPostAt = time.Now()
	p.mu.Unlock()

	res, _ := p.send(ctx, pub, "/bots/"+botID+"/stats", statsBody{count, shardCount}, false, p.cfg.statsInterval)

	p.mu.Lock()
	if res.OK {
		p.lastCount, p.lastShardCount = count, shardCount
	}
	if pub.disabled == "" {
		p.scheduleLocked(pub, p.cfg.statsInterval, p.tickStats)
	}
	p.mu.Unlock()
	return res
}

// measure returns false when the count cannot be trusted, so the caller
// retries later rather than publishing a wrong number.
func (p *Poster) measure(ctx context.Context) (int, bool) {
	if p.cfg.serverCount != nil {
		n, err := p.cfg.serverCount(ctx)
		return n, err == nil && n >= 0
	}
	if !p.topo.localTotal {
		return 0, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	total := 0
	for _, sh := range p.shards {
		if !sh.everReady {
			return 0, false
		}
		n, _ := sh.guilds()
		total += n
	}
	return total, true
}

func (sh *shard) guilds() (int, bool) {
	s := sh.session
	if !s.StateEnabled || s.State == nil {
		return 0, false
	}
	s.State.RLock()
	defer s.State.RUnlock()
	return len(s.State.Guilds) + len(sh.outage), true
}

type shardReport struct {
	ID        int    `json:"id"`
	Status    string `json:"status"`
	LatencyMS *int   `json:"latency_ms,omitempty"`
	Guilds    *int   `json:"guilds,omitempty"`
}

type heartbeatBody struct {
	Cluster      int           `json:"cluster"`
	ClusterCount int           `json:"cluster_count"`
	Shards       []shardReport `json:"shards"`
}

func (p *Poster) collectShardsLocked() []shardReport {
	reports := make([]shardReport, 0, len(p.shards))
	for _, sh := range p.shards {
		report := shardReport{ID: sh.session.ShardID, Status: sh.status}
		// Before the first gateway heartbeat LastHeartbeatSent is zero and the
		// latency is centuries; between a send and its ACK it is negative.
		if latency := sh.session.HeartbeatLatency(); latency >= 0 && latency <= maxLatency {
			ms := int(latency.Milliseconds())
			report.LatencyMS = &ms
		}
		if n, ok := sh.guilds(); ok {
			report.Guilds = &n
		}
		reports = append(reports, report)
	}
	return reports
}

func (p *Poster) beat(ctx context.Context, manual bool) Result {
	pub := p.heartbeat
	if !pub.sending.TryLock() {
		return Result{Skipped: "in-flight"}
	}
	defer pub.sending.Unlock()

	p.mu.Lock()
	if !manual && pub.disabled != "" {
		p.mu.Unlock()
		return Result{Skipped: "disabled"}
	}
	start := time.Now()
	if !manual && start.Before(pub.suspendedUntil) {
		p.scheduleLocked(pub, pub.suspendedUntil.Sub(start), p.tickHeartbeat)
		p.mu.Unlock()
		return Result{Skipped: "plan"}
	}
	body := heartbeatBody{Cluster: p.topo.clusterID, ClusterCount: p.topo.clusterCount, Shards: p.collectShardsLocked()}
	botID, interval := p.botID, p.cfg.heartbeatInterval
	p.mu.Unlock()

	res, respBody := p.send(ctx, pub, "/bots/"+botID+"/heartbeat", body, true, interval)

	p.mu.Lock()
	defer p.mu.Unlock()
	// The cadence follows next_beat_within_seconds, so a change to the
	// server's grace window retunes existing installs without a release.
	var reply struct {
		NextBeatWithinSeconds float64 `json:"next_beat_within_seconds"`
	}
	if res.OK && json.Unmarshal(respBody, &reply) == nil && reply.NextBeatWithinSeconds > 0 {
		proposed := time.Duration(reply.NextBeatWithinSeconds / 3 * float64(time.Second))
		p.cfg.heartbeatInterval = min(max(proposed, heartbeatMin), heartbeatMax)
	}
	if p.beating && pub.disabled == "" {
		jitter := time.Duration(rand.Int64N(int64(heartbeatJitter)))
		delay := max(p.cfg.heartbeatInterval+jitter-time.Since(start), time.Second, time.Until(pub.suspendedUntil))
		p.scheduleLocked(pub, delay, p.tickHeartbeat)
	}
	return res
}

type commandsBody struct {
	Commands []*discordgo.ApplicationCommand `json:"commands"`
}

func (p *Poster) publishCommands(ctx context.Context) Result {
	pub := p.commands
	if !pub.sending.TryLock() {
		return Result{Skipped: "in-flight"}
	}
	defer pub.sending.Unlock()

	p.mu.Lock()
	disabled, suspended, botID := pub.disabled != "", time.Now().Before(pub.suspendedUntil), p.botID
	p.mu.Unlock()
	switch {
	case disabled:
		return Result{Skipped: "disabled"}
	case suspended:
		return Result{Skipped: "plan"}
	}

	cmds, err := p.shards[0].session.ApplicationCommands(botID, "", discordgo.WithContext(ctx))
	if err != nil {
		p.emitError(&Error{Kind: KindCommands, Err: fmt.Errorf("fetching commands from Discord: %w", err)})
		return Result{Skipped: "fetch-failed"}
	}
	// An empty list CLEARS the listing's commands, so it is never sent by accident.
	if len(cmds) == 0 && !p.cfg.postEmpty {
		return Result{Skipped: "empty"}
	}
	res, _ := p.send(ctx, pub, "/bots/"+botID+"/commands", commandsBody{cmds}, false, 0)
	return res
}

// send retries transient failures after a backoff and a 429 after its wait.
// A delay that would outlast the publisher's next tick is dropped: the next
// tick carries fresher data.
func (p *Poster) send(ctx context.Context, pub *publisher, path string, body any, heartbeat bool, nextTick time.Duration) (Result, []byte) {
	var out outcome
	for attempt := 0; attempt < maxAttempts; attempt++ {
		out = p.api.post(ctx, path, body, heartbeat)
		if (out.kind != outcomeTransient && out.kind != outcomeThrottled) || attempt == maxAttempts-1 {
			break
		}
		delay := out.wait
		if out.kind == outcomeTransient {
			delay = p.cfg.backoff(attempt)
		}
		if nextTick > 0 && delay >= nextTick {
			break
		}
		select {
		case <-time.After(delay):
			continue
		case <-ctx.Done():
		}
		break
	}
	return p.react(pub, out), out.body
}

func (p *Poster) react(pub *publisher, out outcome) Result {
	switch out.kind {
	case outcomeOK:
		p.emitPosted(pub.kind, out.status)
		return Result{OK: true, Status: out.status}
	case outcomeDropped:
		return Result{Skipped: "suspended"}
	case outcomePayload:
		// The publisher stays enabled: the next tick may carry valid data.
		err := out.err
		if err == nil {
			err = fmt.Errorf("TheDiscordList rejected the %s payload", pub.kind)
		}
		p.reportOnce(pub, out.status, err)
		return Result{Status: out.status}
	case outcomeAuth:
		for _, each := range p.publishers() {
			p.disable(each, out.status, "unauthorized")
		}
		return Result{Status: out.status}
	case outcomePlan:
		p.mu.Lock()
		pub.suspendedUntil = time.Now().Add(planReprobe)
		p.mu.Unlock()
		p.reportOnce(pub, out.status, fmt.Errorf("the %s endpoint needs a higher plan; retrying in 30 minutes", pub.kind))
		return Result{Status: out.status, Skipped: "plan"}
	case outcomeConfig:
		reason := "not-found"
		if out.status == 409 {
			reason = "conflict"
		}
		p.disable(pub, out.status, reason)
		return Result{Status: out.status}
	case outcomeThrottled:
		return Result{Status: out.status, Skipped: "throttled"}
	default:
		p.emitError(&Error{Kind: pub.kind, Status: out.status, Retryable: true, Err: out.err})
		return Result{Status: out.status}
	}
}

func (p *Poster) reportOnce(pub *publisher, status int, err error) {
	p.mu.Lock()
	seen := pub.reported[status]
	pub.reported[status] = true
	p.mu.Unlock()
	if !seen {
		p.emitError(&Error{Kind: pub.kind, Status: status, Err: err})
	}
}
