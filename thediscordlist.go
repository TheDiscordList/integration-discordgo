// Package thediscordlist publishes a discordgo bot's server count, shard
// heartbeats and slash commands to TheDiscordList.
//
//	session, _ := discordgo.New("Bot " + os.Getenv("DISCORD_TOKEN"))
//	poster := thediscordlist.Autopost(session, os.Getenv("TDL_TOKEN"))
//	defer poster.Stop()
//	session.Open()
//
// Nothing in this package panics into the host process: background loops
// recover, and every Post method returns a [Result] instead of an error.
// Webhook verification lives in the separate webhooks subpackage.
package thediscordlist

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
)

// DefaultBaseURL is the TheDiscordList API root.
const DefaultBaseURL = "https://thediscordlist.com/api"

// Kind names one publisher.
type Kind string

const (
	KindStats     Kind = "stats"
	KindHeartbeat Kind = "heartbeat"
	KindCommands  Kind = "commands"
)

// Result describes one publish attempt.
type Result struct {
	OK bool
	// Status is the HTTP status, or 0 when no response arrived.
	Status int
	// Skipped is set when nothing was sent: "unchanged", "min-gap",
	// "not-elected", "not-ready", "suspended", "plan", "empty", "disabled", …
	Skipped string
}

// Error is reported through [OnError] for failures that did not disable a
// publisher.
type Error struct {
	Kind Kind
	// Status is the HTTP status, or 0 for a network failure.
	Status    int
	Retryable bool
	Err       error
}

func (e *Error) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("thediscordlist %s (%d): %v", e.Kind, e.Status, e.Err)
	}
	return fmt.Sprintf("thediscordlist %s: %v", e.Kind, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// Cluster names this process within a multi-process bot.
type Cluster struct {
	ID    int
	Count int
}

type config struct {
	baseURL     string
	botID       string
	stats       bool
	heartbeat   bool
	commands    bool
	postEmpty   bool
	cluster     *Cluster
	serverCount func(context.Context) (int, error)
	logger      *slog.Logger
	onPosted    func(Kind, int)
	onError     func(*Error)
	onDisabled  func(Kind, int, string)

	statsInterval     time.Duration
	statsDebounce     time.Duration
	statsMinGap       time.Duration
	statsFirstDelay   time.Duration
	statsRemeasure    time.Duration
	heartbeatInterval time.Duration
	commandsDelay     time.Duration
	backoff           func(attempt int) time.Duration
}

// Option configures [Autopost].
type Option func(*config)

// WithBaseURL overrides the API root. Default [DefaultBaseURL].
func WithBaseURL(url string) Option {
	return func(c *config) { c.baseURL = strings.TrimRight(url, "/") }
}

// WithBotID sets the Discord application id. By default it is read from the
// READY event. Setting it lets heartbeats start before the gateway connects,
// so a booting bot reads "connecting" rather than down.
func WithBotID(id string) Option { return func(c *config) { c.botID = id } }

// WithStatsInterval sets the stats cadence. Default 5 minutes.
func WithStatsInterval(d time.Duration) Option { return func(c *config) { c.statsInterval = d } }

// WithHeartbeatInterval sets the starting heartbeat cadence, clamped to 15–60
// seconds. It is retuned from the API's response. Default 30 seconds.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(c *config) { c.heartbeatInterval = d }
}

// WithoutStats disables the stats publisher.
func WithoutStats() Option { return func(c *config) { c.stats = false } }

// WithoutHeartbeat disables the heartbeat publisher.
func WithoutHeartbeat() Option { return func(c *config) { c.heartbeat = false } }

// WithoutCommands disables the commands publisher.
func WithoutCommands() Option { return func(c *config) { c.commands = false } }

// WithPostEmptyCommands allows posting an empty command list, which CLEARS
// the listing's commands.
func WithPostEmptyCommands() Option { return func(c *config) { c.postEmpty = true } }

// WithCluster names this process for a bot split across processes. Only
// cluster 0 posts stats and commands; every cluster heartbeats for itself.
func WithCluster(id, count int) Option {
	return func(c *config) { c.cluster = &Cluster{ID: id, Count: count} }
}

// WithServerCount supplies the bot-wide guild total, for bots whose shards
// live in more than one process. A returned error skips that post.
func WithServerCount(fn func(ctx context.Context) (int, error)) Option {
	return func(c *config) { c.serverCount = fn }
}

// WithLogger sets the logger for warnings, and for errors and disables that
// have no callback. Default [slog.Default].
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// OnPosted is called after every successful POST.
func OnPosted(fn func(kind Kind, status int)) Option { return func(c *config) { c.onPosted = fn } }

// OnError is called for failures that leave the publisher running.
func OnError(fn func(err *Error)) Option { return func(c *config) { c.onError = fn } }

// OnDisabled is called once when a publisher switches itself off. Reasons:
// "unauthorized", "not-found", "conflict", "cluster-count-unknown",
// "server-count-unknown".
func OnDisabled(fn func(kind Kind, status int, reason string)) Option {
	return func(c *config) { c.onDisabled = fn }
}

type shard struct {
	session   *discordgo.Session
	status    string
	everReady bool
	// Guilds that went unavailable in an outage. discordgo drops them from
	// State, but the bot is still in them, so they still count.
	outage map[string]struct{}
}

type topology struct {
	clusterID    int
	clusterCount int // 0 when it cannot be determined
	elected      bool
	shardCount   int
	localTotal   bool
}

type publisher struct {
	kind           Kind
	timer          *time.Timer
	disabled       string
	suspendedUntil time.Time
	reported       map[int]bool
	sending        sync.Mutex
}

// Poster runs the publishers for one process. All methods are safe for
// concurrent use, and every method is a no-op on an inert Poster.
type Poster struct {
	cfg    config
	api    *apiClient
	ctx    context.Context
	cancel context.CancelFunc
	shards []*shard
	topo   topology

	stats, heartbeat, commands *publisher

	mu             sync.Mutex
	botID          string
	started        bool
	beating        bool
	stopped        bool
	detach         []func()
	warned         map[string]bool
	lastPostAt     time.Time
	lastCount      int
	lastShardCount int
	debounceCap    time.Time
	lastBeatAt     time.Time
	beatInterval   time.Duration
}

// Autopost wires one discordgo session to a TheDiscordList listing. Call it
// before or after session.Open.
//
// A discordgo Session is one shard. A process that runs several sessions
// must pass all of them to [AutopostShards] instead.
//
// With an empty token it logs a warning and returns an inert Poster, so a
// missing environment variable never takes down a deploy.
func Autopost(session *discordgo.Session, token string, opts ...Option) *Poster {
	return AutopostShards([]*discordgo.Session{session}, token, opts...)
}

// AutopostShards wires every session (shard) this process runs. When the
// sessions cover all of ShardCount, the process is the whole bot and needs no
// further configuration. Otherwise pass [WithCluster] and [WithServerCount].
func AutopostShards(sessions []*discordgo.Session, token string, opts ...Option) *Poster {
	cfg := config{
		baseURL:           DefaultBaseURL,
		stats:             true,
		heartbeat:         true,
		commands:          true,
		logger:            slog.Default(),
		statsInterval:     statsInterval,
		statsDebounce:     statsDebounce,
		statsMinGap:       statsMinGap,
		statsFirstDelay:   statsFirstDelay,
		statsRemeasure:    statsRemeasure,
		heartbeatInterval: heartbeatInterval,
		commandsDelay:     commandsDelay,
		backoff:           backoff,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	p := &Poster{cfg: cfg, warned: map[string]bool{}, lastCount: -1,
		beatInterval: min(max(cfg.heartbeatInterval, heartbeatMin), heartbeatMax)}
	p.ctx, p.cancel = context.WithCancel(context.Background())

	token = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(token), "Bearer "))
	if token == "" {
		p.warn("no-token", "no stats token supplied; auto-posting is disabled")
		return p
	}
	for _, s := range sessions {
		if s != nil {
			p.shards = append(p.shards, &shard{session: s, status: "connecting", outage: map[string]struct{}{}})
		}
	}
	if len(p.shards) == 0 {
		p.warn("no-session", "no discordgo session supplied; auto-posting is disabled")
		return p
	}

	p.api = newAPIClient(cfg.baseURL, token)
	p.topo = detectTopology(p.shards, cfg.cluster)
	p.botID = cfg.botID
	p.inspectSessions()

	if cfg.stats {
		p.stats = &publisher{kind: KindStats, reported: map[int]bool{}}
		if p.topo.elected && !p.topo.localTotal && cfg.serverCount == nil {
			p.warn("server-count", "this process cannot count the bot's guilds on its own (it does not hold "+
				"every shard, or session state is disabled), so stats are disabled. Pass "+
				"thediscordlist.WithServerCount to enable them")
			p.disable(p.stats, 0, "server-count-unknown")
		}
	}
	if cfg.heartbeat {
		p.heartbeat = &publisher{kind: KindHeartbeat, reported: map[int]bool{}}
		if p.topo.clusterCount == 0 {
			p.warn("cluster-count", "could not determine how many processes this bot runs. Heartbeats are "+
				"disabled, because a wrong cluster_count deletes sibling clusters server-side. Pass "+
				"thediscordlist.WithCluster(id, count) to enable them")
			p.disable(p.heartbeat, 0, "cluster-count-unknown")
		}
	}
	if cfg.commands {
		p.commands = &publisher{kind: KindCommands, reported: map[int]bool{}}
	}

	for _, sh := range p.shards {
		s := sh.session
		p.detach = append(p.detach,
			s.AddHandler(func(_ *discordgo.Session, e *discordgo.Ready) { p.onReady(sh, e) }),
			s.AddHandler(func(_ *discordgo.Session, _ *discordgo.Resumed) { p.onStatus(sh, "ready") }),
			s.AddHandler(func(_ *discordgo.Session, _ *discordgo.Connect) { p.onStatus(sh, "ready") }),
			s.AddHandler(func(s *discordgo.Session, _ *discordgo.Disconnect) {
				// discordgo reconnects on its own unless told not to.
				if s.ShouldReconnectOnError {
					p.onStatus(sh, "connecting")
				} else {
					p.onStatus(sh, "disconnected")
				}
			}),
			s.AddHandler(func(_ *discordgo.Session, e *discordgo.GuildCreate) { p.onGuildCreate(sh, e) }),
			s.AddHandler(func(_ *discordgo.Session, e *discordgo.GuildDelete) { p.onGuildDelete(sh, e) }),
		)
	}

	p.mu.Lock()
	p.startHeartbeatLocked()
	for _, sh := range p.shards {
		if sh.everReady {
			p.startAllLocked()
			break
		}
	}
	p.mu.Unlock()
	return p
}

func detectTopology(shards []*shard, cluster *Cluster) topology {
	total := max(shards[0].session.ShardCount, 1)
	complete := len(shards) == total
	stateful := true
	holdsZero := false
	seen := map[int]bool{}
	for _, sh := range shards {
		s := sh.session
		if max(s.ShardCount, 1) != total || s.ShardID < 0 || s.ShardID >= total || seen[s.ShardID] {
			complete = false
		}
		seen[s.ShardID] = true
		holdsZero = holdsZero || s.ShardID == 0
		stateful = stateful && s.StateEnabled && s.State != nil
	}

	t := topology{shardCount: total, localTotal: complete && stateful}
	switch {
	case cluster != nil:
		if cluster.ID >= 0 && cluster.ID < cluster.Count && cluster.Count <= 1024 {
			t.clusterID, t.clusterCount = cluster.ID, cluster.Count
		}
		t.elected = cluster.ID == 0
	case complete:
		t.clusterCount = 1
		t.elected = true
	default:
		t.elected = holdsZero
	}
	return t
}

// inspectSessions picks up sessions that were opened before Autopost, and
// warns about settings that silently freeze the guild count.
func (p *Poster) inspectSessions() {
	for _, sh := range p.shards {
		s := sh.session
		if s.Identify.Intents&discordgo.IntentsGuilds == 0 {
			p.warn("intents", "discordgo.IntentsGuilds is missing, so guild join and leave events never "+
				"arrive and the reported server count will silently drift")
		}
		if (!s.StateEnabled || s.State == nil) && p.cfg.serverCount == nil {
			p.warn("state", "session state is disabled, so the guild cache stays empty; pass "+
				"thediscordlist.WithServerCount or enable session.StateEnabled")
		}
		if s.State == nil {
			continue
		}
		s.State.RLock()
		if s.State.User != nil {
			sh.status, sh.everReady = "ready", true
			if p.botID == "" {
				p.botID = readyBotID(&s.State.Ready)
			}
		}
		s.State.RUnlock()
	}
}

func readyBotID(r *discordgo.Ready) string {
	if r.Application != nil && r.Application.ID != "" {
		return r.Application.ID
	}
	if r.User != nil {
		return r.User.ID
	}
	return ""
}

func (p *Poster) onReady(sh *shard, r *discordgo.Ready) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sh.status, sh.everReady = "ready", true
	clear(sh.outage)
	if p.botID == "" && r != nil {
		p.botID = readyBotID(r)
	}
	if p.botID == "" {
		p.warnLocked("no-bot-id", "could not read the application id from READY; pass thediscordlist.WithBotID")
		return
	}
	wasBeating := p.beating
	p.startAllLocked()
	if wasBeating {
		p.beatSoonLocked()
	}
}

func (p *Poster) onStatus(sh *shard, status string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if sh.status != status {
		sh.status = status
		p.beatSoonLocked()
	}
}

func (p *Poster) onGuildCreate(sh *shard, e *discordgo.GuildCreate) {
	if e.Guild == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, back := sh.outage[e.ID]; back {
		delete(sh.outage, e.ID)
		return
	}
	p.guildChangedLocked()
}

func (p *Poster) onGuildDelete(sh *shard, e *discordgo.GuildDelete) {
	if e.Guild == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if e.Unavailable {
		if e.BeforeDelete != nil {
			sh.outage[e.ID] = struct{}{}
		}
		return
	}
	delete(sh.outage, e.ID)
	p.guildChangedLocked()
}

// PostStats posts the server count now, re-arming a disabled stats publisher.
// It still honours the 60-second floor between two stats posts.
func (p *Poster) PostStats(ctx context.Context) (res Result) {
	defer p.recoverInto(KindStats, &res)
	if p.stats == nil {
		return Result{Skipped: "disabled"}
	}
	p.mu.Lock()
	botID := p.botID
	p.mu.Unlock()
	switch {
	case botID == "":
		return Result{Skipped: "not-ready"}
	case !p.topo.elected:
		return Result{Skipped: "not-elected"}
	case !p.topo.localTotal && p.cfg.serverCount == nil:
		return Result{Skipped: "server-count-unknown"}
	}
	p.mu.Lock()
	p.stats.disabled = ""
	p.mu.Unlock()
	return p.publishStats(ctx, true)
}

// PostHeartbeat sends a heartbeat now, re-arming a disabled heartbeat
// publisher.
func (p *Poster) PostHeartbeat(ctx context.Context) (res Result) {
	defer p.recoverInto(KindHeartbeat, &res)
	if p.heartbeat == nil {
		return Result{Skipped: "disabled"}
	}
	p.mu.Lock()
	botID, known := p.botID, p.topo.clusterCount != 0
	if botID != "" && known {
		p.heartbeat.disabled = ""
	}
	p.mu.Unlock()
	switch {
	case botID == "":
		return Result{Skipped: "not-ready"}
	case !known:
		return Result{Skipped: "cluster-count-unknown"}
	}
	return p.beat(ctx, true)
}

// PostCommands posts the registered global application commands now,
// re-arming a disabled commands publisher.
func (p *Poster) PostCommands(ctx context.Context) (res Result) {
	defer p.recoverInto(KindCommands, &res)
	if p.commands == nil {
		return Result{Skipped: "disabled"}
	}
	p.mu.Lock()
	botID := p.botID
	if botID != "" {
		p.commands.disabled = ""
	}
	p.mu.Unlock()
	if botID == "" {
		return Result{Skipped: "not-ready"}
	}
	return p.publishCommands(ctx)
}

// Stop cancels every timer and removes every session handler. It is not
// restartable; manual Post calls keep working.
func (p *Poster) Stop() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	for _, pub := range p.publishers() {
		if pub.timer != nil {
			pub.timer.Stop()
		}
	}
	detach := p.detach
	p.detach = nil
	p.mu.Unlock()

	p.cancel()
	for _, remove := range detach {
		remove()
	}
}

func (p *Poster) publishers() []*publisher {
	var all []*publisher
	for _, pub := range []*publisher{p.stats, p.heartbeat, p.commands} {
		if pub != nil {
			all = append(all, pub)
		}
	}
	return all
}

func (p *Poster) warn(key, msg string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.warnLocked(key, msg)
}

func (p *Poster) warnLocked(key, msg string) {
	if !p.warned[key] {
		p.warned[key] = true
		p.cfg.logger.Warn("thediscordlist: " + msg)
	}
}

func (p *Poster) emitPosted(kind Kind, status int) {
	if p.cfg.onPosted != nil {
		p.cfg.onPosted(kind, status)
	}
}

func (p *Poster) emitError(err *Error) {
	if p.cfg.onError != nil {
		p.cfg.onError(err)
		return
	}
	p.cfg.logger.Warn(err.Error())
}

func (p *Poster) emitDisabled(kind Kind, status int, reason string) {
	if p.cfg.onDisabled != nil {
		p.cfg.onDisabled(kind, status, reason)
		return
	}
	p.cfg.logger.Warn("thediscordlist: publisher disabled", "kind", kind, "status", status, "reason", reason)
}

func (p *Poster) recoverInto(kind Kind, res *Result) {
	if r := recover(); r != nil {
		p.cfg.logger.Error("thediscordlist: recovered from a panic", "kind", kind, "panic", r)
		*res = Result{Skipped: "panic"}
	}
}
