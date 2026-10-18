package thediscordlist

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

const modulePath = "github.com/TheDiscordList/integration-discordgo"

type outcomeKind int

const (
	outcomeOK outcomeKind = iota
	outcomePayload
	outcomeAuth
	outcomePlan
	outcomeConfig
	outcomeThrottled
	outcomeTransient
	outcomeDropped
)

type outcome struct {
	kind   outcomeKind
	status int
	body   []byte
	err    error
	// wait is how long a 429 asked the client to back off.
	wait time.Duration
}

// apiClient is shared by every publisher. The server keeps one rate-limit
// counter per client IP across all bot routes, so a 429 suspends stats and
// commands together. Heartbeats bypass the suspension: a dropped stats post is
// a stale number, a dropped heartbeat is a false "down" on the status page.
type apiClient struct {
	http      *http.Client
	baseURL   string
	token     string
	userAgent string

	mu             sync.Mutex
	suspendedUntil time.Time
}

func newAPIClient(baseURL, token string) *apiClient {
	return &apiClient{
		http:      &http.Client{Timeout: requestTimeout},
		baseURL:   baseURL,
		token:     token,
		userAgent: "TheDiscordList-discordgo/" + moduleVersion() + " (+https://" + modulePath + ")",
	}
}

// moduleVersion reads the version Go resolved for this module, so the
// User-Agent never drifts from the published tag.
func moduleVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if dep.Path == modulePath && dep.Version != "" {
				return strings.TrimPrefix(dep.Version, "v")
			}
		}
	}
	return "dev"
}

func (c *apiClient) post(ctx context.Context, path string, payload any, heartbeat bool) outcome {
	if !heartbeat {
		c.mu.Lock()
		until := c.suspendedUntil
		c.mu.Unlock()
		if time.Now().Before(until) {
			return outcome{kind: outcomeDropped}
		}
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return outcome{kind: outcomePayload, err: err}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return outcome{kind: outcomeTransient, err: err}
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.http.Do(req)
	if err != nil {
		return outcome{kind: outcomeTransient, err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return outcome{kind: outcomeTransient, status: resp.StatusCode, err: err}
	}

	out := classify(resp.StatusCode, body)
	if out.kind == outcomeThrottled {
		out.wait = c.suspend(resp.Header)
	}
	return out
}

// suspend honours Retry-After and X-RateLimit-Reset, whichever is later, up
// to an hour. The deadline is taken from time.Now so it stays monotonic.
func (c *apiClient) suspend(h http.Header) time.Duration {
	wait := time.Minute
	if seconds, err := strconv.ParseFloat(h.Get("Retry-After"), 64); err == nil && seconds > 0 {
		wait = time.Duration(min(seconds, maxThrottle.Seconds()) * float64(time.Second))
	}
	if reset, err := strconv.ParseInt(h.Get("X-RateLimit-Reset"), 10, 64); err == nil && reset > 0 {
		wait = max(wait, time.Until(time.Unix(reset, 0)))
	}
	wait = min(wait, maxThrottle) + time.Duration(rand.Int64N(int64(2*time.Second)))
	until := time.Now().Add(wait)

	c.mu.Lock()
	if until.After(c.suspendedUntil) {
		c.suspendedUntil = until
	}
	c.mu.Unlock()
	return wait
}

// classify mirrors the server's precedence 404 → 429 → 401 → 422 → 403/409.
// 404 means "unknown id, or a server listing" and is never reported as a bad
// token, so the API cannot be used to enumerate listings.
func classify(status int, body []byte) outcome {
	switch {
	case status >= 200 && status < 300:
		return outcome{kind: outcomeOK, status: status, body: body}
	case status == http.StatusUnauthorized:
		return outcome{kind: outcomeAuth, status: status}
	case status == http.StatusForbidden:
		return outcome{kind: outcomePlan, status: status}
	case status == http.StatusNotFound, status == http.StatusConflict:
		return outcome{kind: outcomeConfig, status: status}
	case status == http.StatusTooManyRequests:
		return outcome{kind: outcomeThrottled, status: status}
	// Every other 4xx would fail the same way again, except a timeout or "too early".
	case status >= 400 && status < 500 && status != http.StatusRequestTimeout && status != http.StatusTooEarly:
		return outcome{kind: outcomePayload, status: status}
	default:
		return outcome{kind: outcomeTransient, status: status, err: fmt.Errorf("TheDiscordList responded %d", status)}
	}
}

// backoff is min(1s·2^attempt, 30s), scaled into its upper half at random.
func backoff(attempt int) time.Duration {
	d := min(time.Second<<attempt, 30*time.Second)
	return time.Duration(float64(d) * (0.5 + rand.Float64()*0.5))
}
