// Package webhooks verifies and serves the webhooks TheDiscordList sends.
//
// It imports only the standard library, so a webhook-only service never
// compiles discordgo.
package webhooks

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	SignatureHeader = "X-TDL-Signature"
	DeliveryHeader  = "X-TDL-Delivery"
	AttemptHeader   = "X-TDL-Attempt"
	EventHeader     = "X-TDL-Event"

	DefaultTolerance = 5 * time.Minute
	DefaultTimeout   = 8 * time.Second
	MaxBodyBytes     = 1 << 20
)

// Event names sent today. Unknown names still parse: always answer 2xx for them.
const (
	EventListingBumped    = "listing.bumped"
	EventListingUpvoted   = "listing.upvoted"
	EventServerJoined     = "server.joined" // server listings only
	EventBotAdded         = "bot.added"
	EventBotStatsReported = "bot.stats_reported"
	EventWebhookPing      = "webhook.ping"
)

// ErrorCode says why verification failed.
type ErrorCode string

const (
	MissingSignature   ErrorCode = "MISSING_SIGNATURE"
	MalformedSignature ErrorCode = "MALFORMED_SIGNATURE"
	StaleTimestamp     ErrorCode = "STALE_TIMESTAMP"
	InvalidSignature   ErrorCode = "INVALID_SIGNATURE"
	MalformedBody      ErrorCode = "MALFORMED_BODY"
)

// Error is the only error type [Verify] returns.
type Error struct {
	Code ErrorCode
	msg  string
}

func (e *Error) Error() string { return "thediscordlist webhook: " + e.msg }

// Listing identifies the listing an event belongs to.
type Listing struct {
	// ID is the guild id for a server, the application id for a bot.
	ID   string `json:"id"`
	Type string `json:"type"` // "server" or "bot"
	Name string `json:"name"`
	URL  string `json:"url"`
}

// Event is the envelope every webhook shares. Decode Data with
// json.Unmarshal into the struct matching Event.Event.
type Event struct {
	// ID equals the X-TDL-Delivery header and is stable across retries.
	ID    string `json:"id"`
	Event string `json:"event"`
	// CreatedAt is enqueue time, not send time. Never use it for freshness.
	CreatedAt time.Time       `json:"created_at"`
	Listing   Listing         `json:"listing"`
	Data      json.RawMessage `json:"data"`
}

// User is the account behind an event. ID may be null even when present.
type User struct {
	ID       *string `json:"id"`
	Username string  `json:"username"`
	Avatar   *string `json:"avatar"` // full CDN URL
}

// Bumped is the data of listing.bumped.
type Bumped struct {
	Points     int       `json:"points"`
	BumpedAt   time.Time `json:"bumped_at"`
	NextBumpAt time.Time `json:"next_bump_at"`
	User       *User     `json:"user"`
}

// Upvoted is the data of listing.upvoted.
type Upvoted struct {
	VotedAt      time.Time `json:"voted_at"`
	TotalUpvotes int       `json:"total_upvotes"`
	User         *User     `json:"user"`
}

// ServerJoined is the data of server.joined, a click on the join hand-off.
type ServerJoined struct {
	JoinedAt time.Time `json:"joined_at"`
	User     *User     `json:"user"`
}

// BotAdded is the data of bot.added, a click on the install hand-off;
// Discord never confirms the install completed.
type BotAdded struct {
	AddedAt time.Time `json:"added_at"`
	User    *User     `json:"user"`
}

// StatsReported is the data of bot.stats_reported. It also fires on manual
// edits in the web UI, not only as an echo of your own stats post.
type StatsReported struct {
	GuildCount *int      `json:"guild_count"`
	ShardCount *int      `json:"shard_count"`
	ReportedAt time.Time `json:"reported_at"`
}

// Ping is the data of webhook.ping.
type Ping struct {
	Message string `json:"message"`
}

// Verify checks signatureHeader against body with the default five-minute
// tolerance and returns the parsed envelope.
//
// body must be the exact bytes received. The signature covers bytes, not
// meaning, so a re-serialised payload can never verify.
func Verify(body []byte, signatureHeader, secret string) (*Event, error) {
	return VerifyAt(body, signatureHeader, secret, DefaultTolerance, time.Now())
}

// VerifyAt is [Verify] with an explicit tolerance and clock.
func VerifyAt(body []byte, signatureHeader, secret string, tolerance time.Duration, now time.Time) (*Event, error) {
	if signatureHeader == "" {
		return nil, &Error{MissingSignature, "missing " + SignatureHeader + " header"}
	}

	var ts, sig string
	var haveTS bool
	for _, part := range strings.Split(signatureHeader, ",") {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		switch {
		case key == "t" && !haveTS:
			ts, haveTS = value, true
		case key == "v1" && sig == "" && value != "":
			sig = value
		}
	}
	t, err := strconv.ParseInt(ts, 10, 64)
	if !haveTS || err != nil || sig == "" {
		return nil, &Error{MalformedSignature, "malformed " + SignatureHeader + " header"}
	}

	// Two-sided, so a sender clock running ahead is caught too.
	skew := now.Unix() - t
	if skew < 0 {
		skew = -skew
	}
	if skew > int64(tolerance/time.Second) {
		return nil, &Error{StaleTimestamp, "timestamp outside the tolerance window"}
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(t, 10) + "."))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(strings.ToLower(sig)), []byte(expected)) {
		return nil, &Error{InvalidSignature, "signature did not match"}
	}

	var event Event
	if err := json.Unmarshal(body, &event); err != nil {
		return nil, &Error{MalformedBody, "body is not a valid event: " + err.Error()}
	}
	return &event, nil
}

// Delivery carries the per-request headers.
type Delivery struct {
	// ID is X-TDL-Delivery, stable across every retry: dedupe on it.
	ID string
	// Attempt is X-TDL-Attempt, 1-based.
	Attempt int
}

// Handler is an http.Handler that verifies each delivery and passes it to
// OnEvent. It answers 204 when OnEvent returns nil, 500 when it fails or
// panics (so the delivery is retried), 202 when it overruns Timeout (the work
// carries on), and 400 when verification fails.
type Handler struct {
	Secret string
	// OnEvent must return nil for event names it does not handle.
	OnEvent func(ctx context.Context, event *Event, delivery Delivery) error
	// OnError observes rejected deliveries and OnEvent failures. Optional.
	OnError func(err error)
	// Tolerance defaults to DefaultTolerance.
	Tolerance time.Duration
	// Timeout defaults to DefaultTimeout.
	Timeout time.Duration
}

// NewHandler returns a Handler with the default tolerance and timeout.
func NewHandler(secret string, onEvent func(ctx context.Context, event *Event, delivery Delivery) error) *Handler {
	return &Handler{Secret: secret, OnEvent: onEvent}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	if err != nil {
		h.report(err)
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
		return
	}

	tolerance := h.Tolerance
	if tolerance <= 0 {
		tolerance = DefaultTolerance
	}
	event, err := VerifyAt(body, r.Header.Get(SignatureHeader), h.Secret, tolerance, time.Now())
	if err != nil {
		h.report(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if h.OnEvent == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	delivery := Delivery{ID: r.Header.Get(DeliveryHeader), Attempt: 1}
	if n, err := strconv.Atoi(r.Header.Get(AttemptHeader)); err == nil && n > 0 {
		delivery.Attempt = n
	}

	// The sender disables an endpoint after 10 consecutive failures, so an
	// overrunning handler still answers 2xx and keeps working in the background.
	done := make(chan int, 1)
	ctx := context.WithoutCancel(r.Context())
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				h.report(fmt.Errorf("webhook handler panicked: %v", rec))
				done <- http.StatusInternalServerError
			}
		}()
		if err := h.OnEvent(ctx, event, delivery); err != nil {
			h.report(err)
			done <- http.StatusInternalServerError
			return
		}
		done <- http.StatusNoContent
	}()

	timeout := h.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case status := <-done:
		w.WriteHeader(status)
	case <-timer.C:
		w.WriteHeader(http.StatusAccepted)
	}
}

func (h *Handler) report(err error) {
	if h.OnError != nil {
		h.OnError(err)
	}
}
