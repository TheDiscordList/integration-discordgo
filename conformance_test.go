package thediscordlist

import (
	"encoding/json"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/TheDiscordList/integration-discordgo/webhooks"
)

// testdata/sdk-conformance.json is shared byte-for-byte by every TheDiscordList SDK.
func TestConformance(t *testing.T) {
	raw, err := os.ReadFile("testdata/sdk-conformance.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		WebhookSignature struct {
			Secret           string `json:"secret"`
			ToleranceSeconds int    `json:"tolerance_seconds"`
			Vectors          []struct {
				Name   string `json:"name"`
				Now    int64  `json:"now"`
				Body   string `json:"body"`
				Header string `json:"header"`
				Valid  bool   `json:"valid"`
			} `json:"vectors"`
		} `json:"webhook_signature"`
		Defaults struct {
			BaseURL        string `json:"base_url"`
			TimeoutSeconds int    `json:"timeout_seconds"`
			Retry          struct {
				Attempts           int `json:"attempts"`
				BackoffBaseSeconds int `json:"backoff_base_seconds"`
				BackoffMaxSeconds  int `json:"backoff_max_seconds"`
			} `json:"retry"`
			RateLimit struct {
				DefaultSeconds   int `json:"default_seconds"`
				MaxSeconds       int `json:"max_seconds"`
				JitterMaxSeconds int `json:"jitter_max_seconds"`
			} `json:"rate_limit"`
			ForbiddenReprobeSeconds int `json:"forbidden_reprobe_seconds"`
			Stats                   struct {
				FirstDelaySeconds int `json:"first_delay_seconds"`
				IntervalSeconds   int `json:"interval_seconds"`
				DebounceSeconds   int `json:"debounce_seconds"`
				MinGapSeconds     int `json:"min_gap_seconds"`
				ForceAfterSeconds int `json:"force_after_seconds"`
			} `json:"stats"`
			Heartbeat struct {
				IntervalSeconds  int `json:"interval_seconds"`
				JitterMaxSeconds int `json:"jitter_max_seconds"`
			} `json:"heartbeat"`
			Commands struct {
				FirstDelaySeconds int `json:"first_delay_seconds"`
				RetrySeconds      int `json:"retry_seconds"`
			} `json:"commands"`
		} `json:"defaults"`
		ShardStates []string `json:"shard_states"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}

	signature := fixture.WebhookSignature
	if len(signature.Vectors) == 0 {
		t.Fatal("no vectors")
	}
	for _, v := range signature.Vectors {
		_, err := webhooks.VerifyAt([]byte(v.Body), v.Header, signature.Secret, webhooks.DefaultTolerance, time.Unix(v.Now, 0))
		if (err == nil) != v.Valid {
			t.Errorf("vector %q: err = %v, want valid = %v", v.Name, err, v.Valid)
		}
	}

	d := fixture.Defaults
	if DefaultBaseURL != d.BaseURL {
		t.Errorf("base_url = %q, fixture says %q", DefaultBaseURL, d.BaseURL)
	}
	if maxAttempts != d.Retry.Attempts {
		t.Errorf("retry.attempts = %d, fixture says %d", maxAttempts, d.Retry.Attempts)
	}
	for name, c := range map[string]struct {
		sdk     time.Duration
		seconds int
	}{
		"webhook_signature.tolerance_seconds": {webhooks.DefaultTolerance, signature.ToleranceSeconds},
		"timeout_seconds":                     {requestTimeout, d.TimeoutSeconds},
		"retry.backoff_base_seconds":          {backoffBase, d.Retry.BackoffBaseSeconds},
		"retry.backoff_max_seconds":           {backoffMax, d.Retry.BackoffMaxSeconds},
		"rate_limit.default_seconds":          {defaultThrottle, d.RateLimit.DefaultSeconds},
		"rate_limit.max_seconds":              {maxThrottle, d.RateLimit.MaxSeconds},
		"rate_limit.jitter_max_seconds":       {throttleJitter, d.RateLimit.JitterMaxSeconds},
		"forbidden_reprobe_seconds":           {planReprobe, d.ForbiddenReprobeSeconds},
		"stats.first_delay_seconds":           {statsFirstDelay, d.Stats.FirstDelaySeconds},
		"stats.interval_seconds":              {statsInterval, d.Stats.IntervalSeconds},
		"stats.debounce_seconds":              {statsDebounce, d.Stats.DebounceSeconds},
		"stats.min_gap_seconds":               {statsMinGap, d.Stats.MinGapSeconds},
		"stats.force_after_seconds":           {statsForceAfter, d.Stats.ForceAfterSeconds},
		"heartbeat.interval_seconds":          {heartbeatInterval, d.Heartbeat.IntervalSeconds},
		"heartbeat.jitter_max_seconds":        {heartbeatJitter, d.Heartbeat.JitterMaxSeconds},
		"commands.first_delay_seconds":        {commandsDelay, d.Commands.FirstDelaySeconds},
		"commands.retry_seconds":              {commandsRetry, d.Commands.RetrySeconds},
	} {
		if c.sdk != time.Duration(c.seconds)*time.Second {
			t.Errorf("%s = %v, fixture says %ds", name, c.sdk, c.seconds)
		}
	}

	for _, state := range []string{shardReady, shardConnecting, shardDisconnected} {
		if !slices.Contains(fixture.ShardStates, state) {
			t.Errorf("shard state %q is not in the fixture's %v", state, fixture.ShardStates)
		}
	}
}
