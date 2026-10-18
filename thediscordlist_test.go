package thediscordlist

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

type reply struct {
	status int
	header map[string]string
	body   string
}

type request struct {
	path   string
	header http.Header
	body   string
}

// fakeAPI serves both TheDiscordList (/bots/...) and Discord's REST API
// (/api/...), which sessions reach through redirectTransport.
type fakeAPI struct {
	srv      *httptest.Server
	mu       sync.Mutex
	reqs     []request
	replies  map[string][]reply
	commands string
}

func newFakeAPI(t *testing.T) *fakeAPI {
	f := &fakeAPI{replies: map[string][]reply{}, commands: "[]"}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeAPI) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.HasPrefix(r.URL.Path, "/api/") {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, f.commands)
		return
	}
	f.reqs = append(f.reqs, request{path: r.URL.Path, header: r.Header.Clone(), body: string(body)})
	kind := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	rep := reply{status: 200, body: "{}"}
	if kind == "heartbeat" {
		rep.body = `{"cluster":0,"status":"operational","shard_count":1,"next_beat_within_seconds":90}`
	}
	if queue := f.replies[kind]; len(queue) > 0 {
		rep, f.replies[kind] = queue[0], queue[1:]
	}
	for k, v := range rep.header {
		w.Header().Set(k, v)
	}
	w.WriteHeader(rep.status)
	io.WriteString(w, rep.body)
}

func (f *fakeAPI) queue(kind string, replies ...reply) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replies[kind] = append(f.replies[kind], replies...)
}

func (f *fakeAPI) requests(kind string) []request {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []request
	for _, r := range f.reqs {
		if kind == "" || strings.HasSuffix(r.path, "/"+kind) {
			out = append(out, r)
		}
	}
	return out
}

type redirectTransport struct{ to *url.URL }

func (rt redirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme, req.URL.Host = rt.to.Scheme, rt.to.Host
	return http.DefaultTransport.RoundTrip(req)
}

func newSession(t *testing.T, f *fakeAPI, shardID, shardCount int, guilds ...string) *discordgo.Session {
	t.Helper()
	s, err := discordgo.New("Bot discord-token")
	if err != nil {
		t.Fatal(err)
	}
	s.ShardID, s.ShardCount = shardID, shardCount
	target, _ := url.Parse(f.srv.URL)
	s.Client = &http.Client{Transport: redirectTransport{target}}
	for _, id := range guilds {
		if err := s.State.GuildAdd(&discordgo.Guild{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

type events struct {
	mu       sync.Mutex
	posted   []Kind
	errors   []*Error
	disabled map[Kind]string
}

func (e *events) options() []Option {
	return []Option{
		OnPosted(func(k Kind, _ int) { e.mu.Lock(); e.posted = append(e.posted, k); e.mu.Unlock() }),
		OnError(func(err *Error) { e.mu.Lock(); e.errors = append(e.errors, err); e.mu.Unlock() }),
		OnDisabled(func(k Kind, _ int, reason string) {
			e.mu.Lock()
			if e.disabled == nil {
				e.disabled = map[Kind]string{}
			}
			e.disabled[k] = reason
			e.mu.Unlock()
		}),
	}
}

func (e *events) snapshot() ([]*Error, map[Kind]string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	disabled := map[Kind]string{}
	for k, v := range e.disabled {
		disabled[k] = v
	}
	return append([]*Error(nil), e.errors...), disabled
}

var quietLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// testOptions keep background loops out of the way and make retries instant.
func testOptions(f *fakeAPI, extra ...Option) []Option {
	return append([]Option{
		WithBaseURL(f.srv.URL),
		WithLogger(quietLogger),
		func(c *config) {
			c.statsFirstDelay = time.Hour
			c.commandsDelay = time.Hour
			c.statsMinGap = 0
			c.backoff = func(int) time.Duration { return time.Millisecond }
		},
	}, extra...)
}

// prime marks the poster ready without starting its background loops, so
// manual posts are the only requests a test sees.
func prime(p *Poster) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.botID = "123"
	for _, sh := range p.shards {
		sh.status, sh.everReady = "ready", true
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMissingTokenIsInert(t *testing.T) {
	f := newFakeAPI(t)
	for _, token := range []string{"", "   "} {
		p := Autopost(newSession(t, f, 0, 1), token, testOptions(f, WithBotID("123"))...)
		ctx := context.Background()
		for kind, res := range map[Kind]Result{
			KindStats:     p.PostStats(ctx),
			KindHeartbeat: p.PostHeartbeat(ctx),
			KindCommands:  p.PostCommands(ctx),
		} {
			if res != (Result{Skipped: "disabled"}) {
				t.Errorf("token %q: %s = %+v, want skipped disabled", token, kind, res)
			}
		}
		p.Stop()
		p.Stop()
	}
	time.Sleep(20 * time.Millisecond)
	if n := len(f.requests("")); n != 0 {
		t.Fatalf("inert poster sent %d requests", n)
	}
}

func TestStatsRequestShaping(t *testing.T) {
	f := newFakeAPI(t)
	p := Autopost(newSession(t, f, 0, 1, "g1", "g2"), " tdl-token ", testOptions(f, WithoutHeartbeat())...)
	prime(p)

	res := p.PostStats(context.Background())
	if !res.OK || res.Status != 200 {
		t.Fatalf("PostStats = %+v", res)
	}
	reqs := f.requests("stats")
	if len(reqs) != 1 {
		t.Fatalf("got %d stats requests", len(reqs))
	}
	r := reqs[0]
	if r.path != "/bots/123/stats" {
		t.Errorf("path = %q", r.path)
	}
	if r.body != `{"server_count":2,"shard_count":1}` {
		t.Errorf("body = %s", r.body)
	}
	for name, want := range map[string]string{
		"Authorization": "Bearer tdl-token",
		"Content-Type":  "application/json",
		"User-Agent":    "TheDiscordList-discordgo/dev (+https://github.com/TheDiscordList/integration-discordgo)",
	} {
		if got := r.header.Get(name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
}

func TestHeartbeatPayload(t *testing.T) {
	f := newFakeAPI(t)
	s0 := newSession(t, f, 0, 2, "g1", "g2")
	s1 := newSession(t, f, 1, 2)
	s0.LastHeartbeatSent = time.Now()
	s0.LastHeartbeatAck = s0.LastHeartbeatSent.Add(42 * time.Millisecond)
	p := AutopostShards([]*discordgo.Session{s0, s1}, "tok", testOptions(f, WithoutStats(), WithoutCommands())...)

	p.onReady(p.shards[0], &discordgo.Ready{User: &discordgo.User{ID: "999"}, Application: &discordgo.Application{ID: "123"}})
	waitFor(t, "the first heartbeat", func() bool { return len(f.requests("heartbeat")) == 1 })

	r := f.requests("heartbeat")[0]
	if r.path != "/bots/123/heartbeat" {
		t.Errorf("path = %q, want the application id from READY", r.path)
	}
	want := `{"cluster":0,"cluster_count":1,"shards":[` +
		`{"id":0,"status":"ready","latency_ms":42,"guilds":2},` +
		`{"id":1,"status":"connecting","guilds":0}]}`
	if r.body != want {
		t.Errorf("body = %s\nwant   %s", r.body, want)
	}
}

func TestCommandsAreForwarded(t *testing.T) {
	f := newFakeAPI(t)
	f.commands = `[{"id":"1","application_id":"123","version":"9","type":1,"name":"ping","description":"Pong",
		"default_member_permissions":"8","nsfw":false,"options":[{"type":3,"name":"word","description":"A word",
		"required":true,"choices":[{"name":"a","value":"a"},{"name":"n","value":2}]},
		{"type":1,"name":"sub","description":"Sub","options":[{"type":4,"name":"n","description":"N"}]}]},
		{"id":"2","type":2,"name":"Inspect","description":"","default_member_permissions":null,"options":null}]`
	p := Autopost(newSession(t, f, 0, 1), "tok", testOptions(f, WithoutHeartbeat(), WithoutStats())...)
	prime(p)

	if res := p.PostCommands(context.Background()); !res.OK {
		t.Fatalf("PostCommands = %+v", res)
	}
	got := f.requests("commands")[0].body
	for _, want := range []string{
		`{"commands":[{"id":"1",`,
		`"name":"ping"`,
		`"default_member_permissions":"8"`,
		`"choices":[{"name":"a","value":"a"},{"name":"n","value":2}]`,
		`"options":[{"type":4,"name":"n","description":"N"`,
		`"type":2,"name":"Inspect"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("body lacks %s: %s", want, got)
		}
	}
}

func TestEmptyCommandsNeedOptIn(t *testing.T) {
	f := newFakeAPI(t)
	p := Autopost(newSession(t, f, 0, 1), "tok", testOptions(f, WithoutHeartbeat(), WithoutStats())...)
	prime(p)
	if res := p.PostCommands(context.Background()); res.Skipped != "empty" {
		t.Fatalf("PostCommands = %+v, want skipped empty", res)
	}
	if n := len(f.requests("commands")); n != 0 {
		t.Fatalf("sent %d commands requests", n)
	}

	p = Autopost(newSession(t, f, 0, 1), "tok", testOptions(f, WithoutHeartbeat(), WithoutStats(), WithPostEmptyCommands())...)
	prime(p)
	if res := p.PostCommands(context.Background()); !res.OK {
		t.Fatalf("PostCommands = %+v", res)
	}
	if body := f.requests("commands")[0].body; body != `{"commands":[]}` {
		t.Fatalf("body = %s", body)
	}
}

func TestStatusHandling(t *testing.T) {
	tests := []struct {
		name         string
		kind         Kind
		replies      []int
		want         Result
		wantRequests int
		wantDisabled map[Kind]string
		wantErrors   int
	}{
		{"401 disables every publisher", KindStats, []int{401}, Result{Status: 401}, 1,
			map[Kind]string{KindStats: "unauthorized", KindHeartbeat: "unauthorized", KindCommands: "unauthorized"}, 0},
		{"403 suspends the heartbeat", KindHeartbeat, []int{403}, Result{Status: 403, Skipped: "plan"}, 1, map[Kind]string{}, 1},
		{"404 disables stats", KindStats, []int{404}, Result{Status: 404}, 1, map[Kind]string{KindStats: "not-found"}, 0},
		{"404 disables the heartbeat", KindHeartbeat, []int{404}, Result{Status: 404}, 1, map[Kind]string{KindHeartbeat: "not-found"}, 0},
		{"409 disables commands", KindCommands, []int{409}, Result{Status: 409}, 1, map[Kind]string{KindCommands: "conflict"}, 0},
		{"422 is reported and stays enabled", KindStats, []int{422}, Result{Status: 422}, 1, map[Kind]string{}, 1},
		{"5xx is retried", KindStats, []int{502, 500, 200}, Result{OK: true, Status: 200}, 3, map[Kind]string{}, 0},
		{"408 and 425 are retried", KindStats, []int{408, 425, 200}, Result{OK: true, Status: 200}, 3, map[Kind]string{}, 0},
		{"other 4xx is reported, not retried", KindStats, []int{413}, Result{Status: 413}, 1, map[Kind]string{}, 1},
		{"5xx gives up after three attempts", KindHeartbeat, []int{503, 503, 503}, Result{Status: 503}, 3, map[Kind]string{}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeAPI(t)
			f.commands = `[{"type":1,"name":"ping","description":"Pong"}]`
			for _, status := range tt.replies {
				f.queue(string(tt.kind), reply{status: status, body: "{}"})
			}
			var ev events
			p := Autopost(newSession(t, f, 0, 1, "g1"), "tok", testOptions(f, ev.options()...)...)
			prime(p)

			post := map[Kind]func(context.Context) Result{
				KindStats: p.PostStats, KindHeartbeat: p.PostHeartbeat, KindCommands: p.PostCommands,
			}[tt.kind]
			if got := post(context.Background()); got != tt.want {
				t.Errorf("result = %+v, want %+v", got, tt.want)
			}
			if n := len(f.requests(string(tt.kind))); n != tt.wantRequests {
				t.Errorf("requests = %d, want %d", n, tt.wantRequests)
			}
			errs, disabled := ev.snapshot()
			if len(errs) != tt.wantErrors {
				t.Errorf("errors = %v, want %d", errs, tt.wantErrors)
			}
			if len(disabled) != len(tt.wantDisabled) {
				t.Errorf("disabled = %v, want %v", disabled, tt.wantDisabled)
			}
			for k, reason := range tt.wantDisabled {
				if disabled[k] != reason {
					t.Errorf("disabled[%s] = %q, want %q", k, disabled[k], reason)
				}
			}
		})
	}
}

func TestPlanSuspensionReprobes(t *testing.T) {
	f := newFakeAPI(t)
	f.queue("heartbeat", reply{status: 403, body: "{}"}, reply{status: 403, body: "{}"})
	var ev events
	p := Autopost(newSession(t, f, 0, 1), "tok", testOptions(f, append(ev.options(), WithoutStats(), WithoutCommands())...)...)
	prime(p)

	p.PostHeartbeat(context.Background())
	p.mu.Lock()
	until := p.heartbeat.suspendedUntil
	p.mu.Unlock()
	if d := time.Until(until); d < 29*time.Minute || d > 31*time.Minute {
		t.Fatalf("suspended for %v, want 30m", d)
	}
	// A scheduled beat during the suspension sends nothing.
	if res := p.beat(context.Background(), false); res.Skipped != "plan" {
		t.Fatalf("scheduled beat = %+v, want skipped plan", res)
	}
	// An explicit post reprobes; the 403 is reported once only.
	p.PostHeartbeat(context.Background())
	if n := len(f.requests("heartbeat")); n != 2 {
		t.Fatalf("requests = %d, want 2", n)
	}
	if errs, disabled := ev.snapshot(); len(errs) != 1 || len(disabled) != 0 {
		t.Fatalf("errors = %v, disabled = %v", errs, disabled)
	}
}

func TestRateLimitSuspension(t *testing.T) {
	tests := []struct {
		name    string
		header  map[string]string
		atLeast time.Duration
		atMost  time.Duration
	}{
		{"Retry-After", map[string]string{"Retry-After": "120"}, 120 * time.Second, 123 * time.Second},
		{"no header defaults to 60s", nil, 60 * time.Second, 63 * time.Second},
		{"X-RateLimit-Reset wins when later", map[string]string{
			"Retry-After": "5", "X-RateLimit-Reset": strconv.FormatInt(time.Now().Add(10*time.Minute).Unix(), 10),
		}, 9 * time.Minute, 11 * time.Minute},
		{"capped at an hour", map[string]string{"Retry-After": "86400"}, time.Hour, time.Hour + 3*time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeAPI(t)
			f.commands = `[{"type":1,"name":"ping","description":"Pong"}]`
			f.queue("stats", reply{status: 429, header: tt.header, body: "{}"})
			// Every wait outlasts the next tick, so the 429 is not retried.
			p := Autopost(newSession(t, f, 0, 1, "g1"), "tok", testOptions(f, func(c *config) { c.statsInterval = 30 * time.Second })...)
			prime(p)

			if res := p.PostStats(context.Background()); res != (Result{Status: 429, Skipped: "throttled"}) {
				t.Fatalf("PostStats = %+v", res)
			}
			p.api.mu.Lock()
			d := time.Until(p.api.suspendedUntil)
			p.api.mu.Unlock()
			if d < tt.atLeast-time.Second || d > tt.atMost {
				t.Fatalf("suspended for %v, want %v..%v", d, tt.atLeast, tt.atMost)
			}

			// Stats and commands share the suspension; heartbeats bypass it.
			if res := p.PostCommands(context.Background()); res.Skipped != "suspended" {
				t.Errorf("PostCommands = %+v, want skipped suspended", res)
			}
			if n := len(f.requests("commands")); n != 0 {
				t.Errorf("commands requests = %d, want 0", n)
			}
			if res := p.PostHeartbeat(context.Background()); !res.OK {
				t.Errorf("PostHeartbeat = %+v, want sent", res)
			}
		})
	}
}

func TestRateLimitIsRetriedBeforeTheNextTick(t *testing.T) {
	f := newFakeAPI(t)
	f.queue("stats", reply{status: 429, header: map[string]string{"Retry-After": "0.01"}, body: "{}"})
	p := Autopost(newSession(t, f, 0, 1, "g1"), "tok", testOptions(f, WithoutHeartbeat())...)
	prime(p)

	if res := p.PostStats(context.Background()); !res.OK {
		t.Fatalf("PostStats = %+v, want sent once the wait passed", res)
	}
	if n := len(f.requests("stats")); n != 2 {
		t.Fatalf("stats requests = %d, want 2", n)
	}
}

func TestUnknownClusterCountSendsNoHeartbeat(t *testing.T) {
	f := newFakeAPI(t)
	var ev events
	// Shard 1 of 4, alone in this process: the cluster count is unknowable.
	p := Autopost(newSession(t, f, 1, 4), "tok", testOptions(f, append(ev.options(), WithBotID("123"))...)...)
	p.onReady(p.shards[0], &discordgo.Ready{User: &discordgo.User{ID: "123"}})

	if res := p.PostHeartbeat(context.Background()); res.Skipped != "cluster-count-unknown" {
		t.Errorf("PostHeartbeat = %+v", res)
	}
	if res := p.PostStats(context.Background()); res.Skipped != "not-elected" {
		t.Errorf("PostStats = %+v", res)
	}
	time.Sleep(50 * time.Millisecond)
	if n := len(f.requests("heartbeat")); n != 0 {
		t.Fatalf("sent %d heartbeats without a cluster count", n)
	}
	if _, disabled := ev.snapshot(); disabled[KindHeartbeat] != "cluster-count-unknown" {
		t.Fatalf("disabled = %v", disabled)
	}
}

func TestPartialProcessNeedsClusterAndServerCount(t *testing.T) {
	f := newFakeAPI(t)
	var ev events
	p := Autopost(newSession(t, f, 0, 2, "g1"), "tok", testOptions(f, ev.options()...)...)
	prime(p)
	if res := p.PostStats(context.Background()); res.Skipped != "server-count-unknown" {
		t.Errorf("PostStats = %+v, want skipped server-count-unknown", res)
	}
	if _, disabled := ev.snapshot(); disabled[KindStats] != "server-count-unknown" || disabled[KindHeartbeat] != "cluster-count-unknown" {
		t.Errorf("disabled = %v", disabled)
	}

	p = Autopost(newSession(t, f, 0, 2, "g1"), "tok", testOptions(f,
		WithCluster(0, 2),
		WithServerCount(func(context.Context) (int, error) { return 1234, nil }),
	)...)
	prime(p)
	if res := p.PostStats(context.Background()); !res.OK {
		t.Fatalf("PostStats = %+v", res)
	}
	if body := f.requests("stats")[0].body; body != `{"server_count":1234,"shard_count":2}` {
		t.Errorf("stats body = %s", body)
	}
	if res := p.PostHeartbeat(context.Background()); !res.OK {
		t.Fatalf("PostHeartbeat = %+v", res)
	}
	if body := f.requests("heartbeat")[0].body; !strings.HasPrefix(body, `{"cluster":0,"cluster_count":2,`) {
		t.Errorf("heartbeat body = %s", body)
	}
}

func TestInvalidClusterSendsNoHeartbeat(t *testing.T) {
	f := newFakeAPI(t)
	for _, c := range []Cluster{{2, 2}, {-1, 2}, {0, 0}, {0, 1025}} {
		p := Autopost(newSession(t, f, 0, 1), "tok", testOptions(f, WithCluster(c.ID, c.Count))...)
		prime(p)
		if res := p.PostHeartbeat(context.Background()); res.Skipped != "cluster-count-unknown" {
			t.Errorf("cluster %+v: PostHeartbeat = %+v", c, res)
		}
	}
}

func TestManualPostRearms(t *testing.T) {
	f := newFakeAPI(t)
	f.queue("stats", reply{status: 404, body: "{}"})
	p := Autopost(newSession(t, f, 0, 1, "g1"), "tok", testOptions(f, WithoutHeartbeat())...)
	prime(p)

	p.PostStats(context.Background())
	if res := p.publishStats(context.Background(), false); res.Skipped != "disabled" {
		t.Fatalf("scheduled post after 404 = %+v, want skipped disabled", res)
	}
	if res := p.PostStats(context.Background()); !res.OK {
		t.Fatalf("manual post after 404 = %+v, want sent", res)
	}
}

func TestStatsMinGapAndUnchanged(t *testing.T) {
	f := newFakeAPI(t)
	s := newSession(t, f, 0, 1, "g1")
	p := Autopost(s, "tok", testOptions(f, WithoutHeartbeat(), func(c *config) { c.statsMinGap = time.Minute })...)
	prime(p)
	ctx := context.Background()

	if res := p.PostStats(ctx); !res.OK {
		t.Fatalf("first = %+v", res)
	}
	if res := p.PostStats(ctx); res.Skipped != "min-gap" {
		t.Fatalf("second = %+v, want skipped min-gap", res)
	}

	p.cfg.statsMinGap = 0
	if res := p.PostStats(ctx); res.Skipped != "unchanged" {
		t.Fatalf("unchanged count = %+v, want skipped unchanged", res)
	}
	s.State.GuildAdd(&discordgo.Guild{ID: "g2"})
	if res := p.PostStats(ctx); !res.OK {
		t.Fatalf("changed count = %+v", res)
	}
	if n := len(f.requests("stats")); n != 2 {
		t.Fatalf("stats requests = %d, want 2", n)
	}
}

func TestOutageKeepsGuildCount(t *testing.T) {
	f := newFakeAPI(t)
	s := newSession(t, f, 0, 1, "g1", "g2")
	p := Autopost(s, "tok", testOptions(f, WithoutHeartbeat())...)
	prime(p)
	sh := p.shards[0]

	// What discordgo's State does before dispatching an unavailable GUILD_DELETE.
	before, _ := s.State.Guild("g2")
	s.State.GuildRemove(&discordgo.Guild{ID: "g2"})
	p.onGuildDelete(sh, &discordgo.GuildDelete{Guild: &discordgo.Guild{ID: "g2", Unavailable: true}, BeforeDelete: before})
	if n, ok := p.measure(context.Background()); !ok || n != 2 {
		t.Fatalf("count during outage = %d, %v; want 2", n, ok)
	}

	s.State.GuildAdd(&discordgo.Guild{ID: "g2"})
	p.onGuildCreate(sh, &discordgo.GuildCreate{Guild: &discordgo.Guild{ID: "g2"}})
	if n, _ := p.measure(context.Background()); n != 2 {
		t.Fatalf("count after recovery = %d, want 2", n)
	}

	s.State.GuildRemove(&discordgo.Guild{ID: "g1"})
	p.onGuildDelete(sh, &discordgo.GuildDelete{Guild: &discordgo.Guild{ID: "g1"}})
	if n, _ := p.measure(context.Background()); n != 1 {
		t.Fatalf("count after a real leave = %d, want 1", n)
	}
}

func TestAutopostLifecycle(t *testing.T) {
	f := newFakeAPI(t)
	f.commands = `[{"type":1,"name":"ping","description":"Pong"}]`
	s := newSession(t, f, 0, 1, "g1")
	var ev events
	p := Autopost(s, "tok", testOptions(f, append(ev.options(), func(c *config) {
		c.statsFirstDelay = 10 * time.Millisecond
		c.commandsDelay = 20 * time.Millisecond
		c.statsDebounce = 10 * time.Millisecond
	})...)...)

	time.Sleep(30 * time.Millisecond)
	if n := len(f.requests("")); n != 0 {
		t.Fatalf("sent %d requests before READY", n)
	}

	p.onReady(p.shards[0], &discordgo.Ready{User: &discordgo.User{ID: "123"}})
	waitFor(t, "heartbeat, stats and commands", func() bool {
		return len(f.requests("heartbeat")) == 1 && len(f.requests("stats")) == 1 && len(f.requests("commands")) == 1
	})

	// A join after the first post is debounced into one more stats post.
	s.State.GuildAdd(&discordgo.Guild{ID: "g2"})
	p.onGuildCreate(p.shards[0], &discordgo.GuildCreate{Guild: &discordgo.Guild{ID: "g2"}})
	waitFor(t, "the debounced stats post", func() bool { return len(f.requests("stats")) == 2 })
	if body := f.requests("stats")[1].body; body != `{"server_count":2,"shard_count":1}` {
		t.Errorf("debounced body = %s", body)
	}

	p.Stop()
	p.onStatus(p.shards[0], "connecting")
	p.onGuildCreate(p.shards[0], &discordgo.GuildCreate{Guild: &discordgo.Guild{ID: "g3"}})
	time.Sleep(1200 * time.Millisecond)
	if n := len(f.requests("")); n != 4 {
		t.Fatalf("requests after Stop = %d, want 4", n)
	}
	if errs, disabled := ev.snapshot(); len(errs) != 0 || len(disabled) != 0 {
		t.Fatalf("errors = %v, disabled = %v", errs, disabled)
	}
}

func TestHeartbeatStartsBeforeReadyWithBotID(t *testing.T) {
	f := newFakeAPI(t)
	p := Autopost(newSession(t, f, 0, 1), "tok", testOptions(f, WithBotID("123"))...)
	defer p.Stop()
	waitFor(t, "a pre-READY heartbeat", func() bool { return len(f.requests("heartbeat")) == 1 })
	if body := f.requests("heartbeat")[0].body; !strings.Contains(body, `"status":"connecting"`) {
		t.Fatalf("body = %s", body)
	}
}

func TestPanickingCallbackIsContained(t *testing.T) {
	f := newFakeAPI(t)
	p := Autopost(newSession(t, f, 0, 1, "g1"), "tok", testOptions(f, WithoutHeartbeat(),
		OnPosted(func(Kind, int) { panic("boom") }))...)
	prime(p)
	if res := p.PostStats(context.Background()); res.Skipped != "panic" {
		t.Fatalf("PostStats = %+v, want skipped panic", res)
	}
	// The in-flight guard was released by the unwinding panic.
	p.cfg.onPosted = nil
	if res := p.PostStats(context.Background()); !res.OK {
		t.Fatalf("PostStats after panic = %+v", res)
	}
}

func TestBackoffBounds(t *testing.T) {
	for attempt, want := range map[int]time.Duration{0: time.Second, 1: 2 * time.Second, 2: 4 * time.Second, 10: 30 * time.Second} {
		for range 50 {
			if d := backoff(attempt); d < want/2 || d > want {
				t.Fatalf("backoff(%d) = %v, want %v..%v", attempt, d, want/2, want)
			}
		}
	}
}
