package webhooks

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The shared vector every TheDiscordList SDK asserts, computed with the
// server's PHP formula.
const (
	vectorSecret = "whsec_test"
	vectorT      = 1700000000
	vectorBody   = `{"id":"d1","event":"webhook.ping","created_at":"2023-11-14T22:13:20+00:00","listing":{"id":"1","type":"bot","name":"Bot","url":"https://thediscordlist.com/bots/1"},"data":{"message":"hi"}}`
	vectorV1     = "b6eeea31d04c3f86990dcbb706dfaf988c9ff2fc234316ae77e168831127dc59"
)

var vectorNow = time.Unix(vectorT, 0)

func sign(secret string, t int64, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(t, 10) + "." + body))
	return hex.EncodeToString(mac.Sum(nil))
}

func TestSharedVector(t *testing.T) {
	if got := sign(vectorSecret, vectorT, vectorBody); got != vectorV1 {
		t.Fatalf("HMAC = %s, want %s", got, vectorV1)
	}
	event, err := VerifyAt([]byte(vectorBody), "t=1700000000,v1="+vectorV1, vectorSecret, DefaultTolerance, vectorNow)
	if err != nil {
		t.Fatal(err)
	}
	var ping Ping
	if err := json.Unmarshal(event.Data, &ping); err != nil {
		t.Fatal(err)
	}
	if event.ID != "d1" || event.Event != EventWebhookPing || event.Listing.Type != "bot" ||
		!event.CreatedAt.Equal(vectorNow) || ping.Message != "hi" {
		t.Fatalf("event = %+v, data = %+v", event, ping)
	}
}

func TestVerify(t *testing.T) {
	good := "t=1700000000,v1=" + vectorV1
	tests := []struct {
		name   string
		body   string
		header string
		secret string
		now    time.Time
		want   ErrorCode
	}{
		{"valid", vectorBody, good, vectorSecret, vectorNow, ""},
		{"valid with spaces", vectorBody, " t=1700000000 , v1=" + vectorV1, vectorSecret, vectorNow, ""},
		{"uppercase hex signature", vectorBody, "t=1700000000,v1=" + strings.ToUpper(vectorV1), vectorSecret, vectorNow, ""},
		{"values containing =", vectorBody, "t=1700000000,v0=abc=def==,v1=" + vectorV1 + ",x==", vectorSecret, vectorNow, ""},
		{"first non-empty v1 wins", vectorBody, "v1=,t=1700000000,v1=" + vectorV1 + ",v1=00", vectorSecret, vectorNow, ""},
		{"first t wins", vectorBody, good + ",t=1", vectorSecret, vectorNow, ""},
		{"edge of the window", vectorBody, good, vectorSecret, vectorNow.Add(DefaultTolerance), ""},
		{"tampered body", strings.Replace(vectorBody, `"hi"`, `"ho"`, 1), good, vectorSecret, vectorNow, InvalidSignature},
		{"wrong secret", vectorBody, good, "whsec_other", vectorNow, InvalidSignature},
		{"truncated signature", vectorBody, "t=1700000000,v1=" + vectorV1[:63], vectorSecret, vectorNow, InvalidSignature},
		{"stale timestamp", vectorBody, good, vectorSecret, vectorNow.Add(DefaultTolerance + time.Second), StaleTimestamp},
		{"timestamp from the future", vectorBody, good, vectorSecret, vectorNow.Add(-DefaultTolerance - time.Second), StaleTimestamp},
		{"missing header", vectorBody, "", vectorSecret, vectorNow, MissingSignature},
		{"no v1", vectorBody, "t=1700000000", vectorSecret, vectorNow, MalformedSignature},
		{"empty v1", vectorBody, "t=1700000000,v1=", vectorSecret, vectorNow, MalformedSignature},
		{"no t", vectorBody, "v1=" + vectorV1, vectorSecret, vectorNow, MalformedSignature},
		{"non-numeric t", vectorBody, "t=abc,v1=" + vectorV1, vectorSecret, vectorNow, MalformedSignature},
		{"non-finite t", vectorBody, "t=Inf,v1=" + vectorV1, vectorSecret, vectorNow, MalformedSignature},
		{"garbage header", vectorBody, "garbage", vectorSecret, vectorNow, MalformedSignature},
		{"body is not JSON", "not json", "t=1700000000,v1=" + sign(vectorSecret, vectorT, "not json"), vectorSecret, vectorNow, MalformedBody},
		{"empty secret", vectorBody, "t=1700000000,v1=" + sign("", vectorT, vectorBody), "", vectorNow, MissingSecret},
		{"blank secret", vectorBody, "t=1700000000,v1=" + sign(" ", vectorT, vectorBody), " ", vectorNow, MissingSecret},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := VerifyAt([]byte(tt.body), tt.header, tt.secret, DefaultTolerance, tt.now)
			if tt.want == "" {
				if err != nil || event == nil {
					t.Fatalf("err = %v", err)
				}
				return
			}
			var verr *Error
			if !errors.As(err, &verr) || verr.Code != tt.want {
				t.Fatalf("err = %v, want code %s", err, tt.want)
			}
		})
	}
}

func TestUnknownEventParses(t *testing.T) {
	body := `{"id":"d2","event":"listing.renamed","created_at":"2026-08-14T12:34:56+00:00",` +
		`"listing":{"id":"9","type":"server","name":"S","url":"https://thediscordlist.com/servers/9"},"data":{"new":true}}`
	now := time.Now()
	event, err := VerifyAt([]byte(body), "t="+strconv.FormatInt(now.Unix(), 10)+",v1="+sign("s", now.Unix(), body), "s", DefaultTolerance, now)
	if err != nil || event.Event != "listing.renamed" || string(event.Data) != `{"new":true}` {
		t.Fatalf("event = %+v, err = %v", event, err)
	}
}

func TestHandlerRefusesAnEmptySecret(t *testing.T) {
	h := NewHandler("", func(context.Context, *Event, Delivery) error {
		t.Error("OnEvent ran without a secret")
		return nil
	})
	var reported error
	h.OnError = func(err error) { reported = err }

	now := time.Now().Unix()
	req := httptest.NewRequest(http.MethodPost, "/webhooks/tdl", strings.NewReader(vectorBody))
	req.Header.Set(SignatureHeader, "t="+strconv.FormatInt(now, 10)+",v1="+sign("", now, vectorBody))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	var verr *Error
	if rec.Code != http.StatusInternalServerError || !errors.As(reported, &verr) || verr.Code != MissingSecret {
		t.Fatalf("status = %d, reported = %v", rec.Code, reported)
	}
}

func TestHandler(t *testing.T) {
	signed := func(body string) http.Header {
		now := time.Now().Unix()
		h := http.Header{}
		h.Set(SignatureHeader, "t="+strconv.FormatInt(now, 10)+",v1="+sign(vectorSecret, now, body))
		h.Set(DeliveryHeader, "8b5e0c1e-0000-4000-8000-000000000001")
		h.Set(AttemptHeader, "3")
		return h
	}
	ping := vectorBody
	unknown := strings.Replace(vectorBody, "webhook.ping", "listing.renamed", 1)
	upvoted := strings.Replace(vectorBody, "webhook.ping", "listing.upvoted", 1)
	malformed := http.Header{}
	malformed.Set(SignatureHeader, "garbage")

	var (
		mu   sync.Mutex
		seen Delivery
	)
	onEvent := func(_ context.Context, event *Event, d Delivery) error {
		mu.Lock()
		seen = d
		mu.Unlock()
		switch event.Event {
		case EventListingUpvoted:
			return errors.New("database down")
		case "slow":
			time.Sleep(200 * time.Millisecond)
		case "panic":
			panic("boom")
		}
		return nil
	}

	tests := []struct {
		name   string
		method string
		body   string
		header http.Header
		want   int
	}{
		{"ping answers 204", http.MethodPost, ping, signed(ping), http.StatusNoContent},
		{"unknown event answers 204", http.MethodPost, unknown, signed(unknown), http.StatusNoContent},
		{"handler error answers 500", http.MethodPost, upvoted, signed(upvoted), http.StatusInternalServerError},
		{"handler panic answers 500", http.MethodPost, strings.Replace(ping, "webhook.ping", "panic", 1),
			signed(strings.Replace(ping, "webhook.ping", "panic", 1)), http.StatusInternalServerError},
		{"overrun answers 202", http.MethodPost, strings.Replace(ping, "webhook.ping", "slow", 1),
			signed(strings.Replace(ping, "webhook.ping", "slow", 1)), http.StatusAccepted},
		{"bad signature answers 401", http.MethodPost, ping, signed(unknown), http.StatusUnauthorized},
		{"missing signature answers 401", http.MethodPost, ping, http.Header{}, http.StatusUnauthorized},
		{"malformed signature answers 400", http.MethodPost, ping, malformed, http.StatusBadRequest},
		{"GET answers 405", http.MethodGet, "", http.Header{}, http.StatusMethodNotAllowed},
		{"oversized body answers 413", http.MethodPost, strings.Repeat("x", MaxBodyBytes+1), http.Header{}, http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler(vectorSecret, onEvent)
			h.Timeout = 50 * time.Millisecond
			var reported error
			h.OnError = func(err error) { reported = err }

			req := httptest.NewRequest(tt.method, "/webhooks/tdl", strings.NewReader(tt.body))
			req.Header = tt.header
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
			if (tt.want == http.StatusBadRequest || tt.want == http.StatusUnauthorized) && reported == nil {
				t.Fatal("verification failure was not reported")
			}
		})
	}
	time.Sleep(250 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if seen.ID != "8b5e0c1e-0000-4000-8000-000000000001" || seen.Attempt != 3 {
		t.Fatalf("delivery = %+v", seen)
	}
}
