/*
Copyright 2018-2022 Mailgun Technologies Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gubernator

import (
	"context"

	"github.com/mailgun/holster/v4/clock"
	"github.com/mailgun/holster/v4/tracing"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

// Implements token bucket algorithm for rate limiting. https://en.wikipedia.org/wiki/Token_bucket
func tokenBucket(ctx context.Context, s Store, c Cache, r *RateLimitReq) (*RateLimitResp, error) {
	var resp *RateLimitResp

	err := tracing.Scope(ctx, func(ctx context.Context) error {
		tokenBucketTimer := prometheus.NewTimer(funcTimeMetric.WithLabelValues("tokenBucket"))
		defer tokenBucketTimer.ObserveDuration()

		// Get rate limit from cache.
		var err error
		span := trace.SpanFromContext(ctx)
		hashKey := r.HashKey()
		item, ok := c.GetItem(hashKey)
		span.AddEvent("c.GetItem()")

		if s != nil && !ok {
			// Cache miss.
			// Check our store for the item.
			if item, ok = s.Get(ctx, r); ok {
				span.AddEvent("Check store for rate limit")
				c.Add(item)
				span.AddEvent("c.Add()")
			}
		}

		// Sanity checks.
		if ok {
			if item.Value == nil {
				logrus.WithContext(ctx).
					WithFields(logrus.Fields{
						"hashKey": hashKey,
						"key": r.UniqueKey,
						"name": r.Name,
					}).
					Error("tokenBucket: Invalid cache item; Value is nil")
				ok = false
			} else if item.Key != hashKey {
				logrus.WithContext(ctx).
					WithFields(logrus.Fields{
						"itemKey": item.Key,
						"hashKey": hashKey,
						"name": r.Name,
					}).
					Error("tokenBucket: Invalid cache item; key mismatch")
				ok = false
			}
		}

		if ok {
			// Item found in cache or store.
			span.AddEvent("Update existing rate limit")

			if HasBehavior(r.Behavior, Behavior_RESET_REMAINING) {
				c.Remove(hashKey)
				span.AddEvent("c.Remove()")

				if s != nil {
					s.Remove(ctx, hashKey)
					span.AddEvent("s.Remove()")
				}
				resp = &RateLimitResp{
					Status:    Status_UNDER_LIMIT,
					Limit:     r.Limit,
					Remaining: r.Limit,
					ResetTime: 0,
				}
				return nil
			}

			// The following semantic allows for requests of more than the limit to be rejected, but subsequent
			// requests within the same duration that are under the limit to succeed. IE: client attempts to
			// send 1000 emails but 100 is their limit. The request is rejected as over the limit, but since we
			// don't store OVER_LIMIT in the cache the client can retry within the same rate limit duration with
			// 100 emails and the request will succeed.
			t, ok := item.Value.(*TokenBucketItem)
			if !ok {
				// Client switched algorithms; perhaps due to a migration?
				span.AddEvent("Client switched algorithms; perhaps due to a migration?")

				c.Remove(hashKey)
				span.AddEvent("c.Remove()")

				if s != nil {
					s.Remove(ctx, hashKey)
					span.AddEvent("s.Remove()")
				}

				resp, err = tokenBucketNewItem(ctx, s, c, r)
				return err
			}

			// Update the limit if it changed.
			span.AddEvent("Update the limit if changed")
			if t.Limit != r.Limit {
				// Add difference to remaining.
				t.Remaining += r.Limit - t.Limit
				if t.Remaining < 0 {
					t.Remaining = 0
				}
				t.Limit = r.Limit
			}

			rl := &RateLimitResp{
				Status:    t.Status,
				Limit:     r.Limit,
				Remaining: t.Remaining,
				ResetTime: item.ExpireAt,
			}

			// If the duration config changed, update the new ExpireAt.
			if t.Duration != r.Duration {
				span.AddEvent("Duration changed")
				expire := t.CreatedAt + r.Duration
				if HasBehavior(r.Behavior, Behavior_DURATION_IS_GREGORIAN) {
					expire, err = GregorianExpiration(clock.Now(), r.Duration)
					if err != nil {
						return errors.Wrap(err, "error in GregorianExpiration")
					}
				}

				// If our new duration means we are currently expired.
				now := MillisecondNow()
				if expire <= now {
					// Renew item.
					span.AddEvent("Limit has expired")
					expire = now + r.Duration
					t.CreatedAt = now
					t.Remaining = t.Limit
				}

				item.ExpireAt = expire
				t.Duration = r.Duration
				rl.ResetTime = expire
			}

			if s != nil {
				defer func() {
					s.OnChange(ctx, r, item)
					span.AddEvent("defer s.OnChange()")
				}()
			}

			// Client is only interested in retrieving the current status or
			// updating the rate limit config.
			if r.Hits == 0 {
				span.AddEvent("Return current status, apply no change")
				resp = rl
				return nil
			}

			// If we are already at the limit.
			if rl.Remaining == 0 {
				span.AddEvent("Already over the limit")
				overLimitCounter.Add(1)
				rl.Status = Status_OVER_LIMIT
				t.Status = rl.Status
				resp = rl
				return nil
			}

			// If requested hits takes the remainder.
			if t.Remaining == r.Hits {
				span.AddEvent("At the limit")
				t.Remaining = 0
				rl.Remaining = 0
				resp = rl
				return nil
			}

			// If requested is more than available, then return over the limit
			// without updating the cache.
			if r.Hits > t.Remaining {
				span.AddEvent("Over the limit")
				overLimitCounter.Add(1)
				rl.Status = Status_OVER_LIMIT
				resp = rl
				return nil
			}

			span.AddEvent("Under the limit")
			t.Remaining -= r.Hits
			rl.Remaining = t.Remaining
			resp = rl
			return nil
		}

		// Item is not found in cache or store, create new.
		resp, err = tokenBucketNewItem(ctx, s, c, r)
		return err
	})

	return resp, err
}

// Called by tokenBucket() when adding a new item in the store.
func tokenBucketNewItem(ctx context.Context, s Store, c Cache, r *RateLimitReq) (*RateLimitResp, error) {
	var resp *RateLimitResp

	err := tracing.Scope(ctx, func(ctx context.Context) error {
		var err error
		span := trace.SpanFromContext(ctx)
		now := MillisecondNow()
		expire := now + r.Duration

		t := &TokenBucketItem{
			Limit:     r.Limit,
			Duration:  r.Duration,
			Remaining: r.Limit - r.Hits,
			CreatedAt: now,
		}
		item := &CacheItem{
			Algorithm: Algorithm_TOKEN_BUCKET,
			Key:       r.HashKey(),
			Value:     t,
			ExpireAt:  expire,
		}

		// Add a new rate limit to the cache.
		span.AddEvent("Add a new rate limit to the cache")
		if HasBehavior(r.Behavior, Behavior_DURATION_IS_GREGORIAN) {
			expire, err = GregorianExpiration(clock.Now(), r.Duration)
			if err != nil {
				return errors.Wrap(err, "error in GregorianExpiration")
			}
		}

		rl := &RateLimitResp{
			Status:    Status_UNDER_LIMIT,
			Limit:     r.Limit,
			Remaining: t.Remaining,
			ResetTime: expire,
		}

		// Client could be requesting that we always return OVER_LIMIT.
		if r.Hits > r.Limit {
			span.AddEvent("Over the limit")
			overLimitCounter.Add(1)
			rl.Status = Status_OVER_LIMIT
			rl.Remaining = r.Limit
			t.Remaining = r.Limit
		}

		c.Add(item)
		span.AddEvent("c.Add()")

		if s != nil {
			s.OnChange(ctx, r, item)
			span.AddEvent("s.OnChange()")
		}

		resp = rl
		return nil
	})

	return resp, err
}

// Implements leaky bucket algorithm for rate limiting https://en.wikipedia.org/wiki/Leaky_bucket
func leakyBucket(ctx context.Context, s Store, c Cache, r *RateLimitReq) (*RateLimitResp, error) {
	var resp *RateLimitResp

	err := tracing.Scope(ctx, func(ctx context.Context) error {
		leakyBucketTimer := prometheus.NewTimer(funcTimeMetric.WithLabelValues("V1Instance.getRateLimit_leakyBucket"))
		defer leakyBucketTimer.ObserveDuration()

		if r.Burst == 0 {
			r.Burst = r.Limit
		}

		var err error
		now := MillisecondNow()
		span := trace.SpanFromContext(ctx)

		// Get rate limit from cache.
		hashKey := r.HashKey()
		item, ok := c.GetItem(hashKey)
		span.AddEvent("c.GetItem()")

		if s != nil && !ok {
			// Cache miss.
			// Check our store for the item.
			if item, ok = s.Get(ctx, r); ok {
				span.AddEvent("Check store for rate limit")
				c.Add(item)
				span.AddEvent("c.Add()")
			}
		}

		// Sanity checks.
		if ok {
			if item.Value == nil {
				logrus.WithContext(ctx).
					WithFields(logrus.Fields{
						"hashKey": hashKey,
						"key": r.UniqueKey,
						"name": r.Name,
					}).
					Error("leakyBucket: Invalid cache item; Value is nil")
				ok = false
			} else if item.Key != hashKey {
				logrus.WithContext(ctx).
					WithFields(logrus.Fields{
						"itemKey": item.Key,
						"hashKey": hashKey,
						"name": r.Name,
					}).
					Error("leakyBucket: Invalid cache item; key mismatch")
				ok = false
			}
		}

		if ok {
			// Item found in cache or store.
			span.AddEvent("Update existing rate limit")

			b, ok := item.Value.(*LeakyBucketItem)
			if !ok {
				// Client switched algorithms; perhaps due to a migration?
				c.Remove(hashKey)
				span.AddEvent("c.Remove()")

				if s != nil {
					s.Remove(ctx, hashKey)
					span.AddEvent("s.Remove()")
				}

				resp, err = leakyBucketNewItem(ctx, s, c, r)
				return err
			}

			if HasBehavior(r.Behavior, Behavior_RESET_REMAINING) {
				b.Remaining = float64(r.Burst)
			}

			// Update burst, limit and duration if they changed
			if b.Burst != r.Burst {
				if r.Burst > int64(b.Remaining) {
					b.Remaining = float64(r.Burst)
				}
				b.Burst = r.Burst
			}

			b.Limit = r.Limit
			b.Duration = r.Duration

			duration := r.Duration
			rate := float64(duration) / float64(r.Limit)

			if HasBehavior(r.Behavior, Behavior_DURATION_IS_GREGORIAN) {
				d, err := GregorianDuration(clock.Now(), r.Duration)
				if err != nil {
					return errors.Wrap(err, "error in GregorianDuration")
				}
				n := clock.Now()
				expire, err := GregorianExpiration(n, r.Duration)
				if err != nil {
					return errors.Wrap(err, "error in GregorianExpiration")
				}

				// Calculate the rate using the entire duration of the gregorian interval
				// IE: Minute = 60,000 milliseconds, etc.. etc..
				rate = float64(d) / float64(r.Limit)
				// Update the duration to be the end of the gregorian interval
				duration = expire - (n.UnixNano() / 1000000)
			}

			if r.Hits != 0 {
				c.UpdateExpiration(r.HashKey(), now+duration)
			}

			// Calculate how much leaked out of the bucket since the last time we leaked a hit
			elapsed := now - b.UpdatedAt
			leak := float64(elapsed) / rate

			if int64(leak) > 0 {
				b.Remaining += leak
				b.UpdatedAt = now
			}

			if int64(b.Remaining) > b.Burst {
				b.Remaining = float64(b.Burst)
			}

			rl := &RateLimitResp{
				Limit:     b.Limit,
				Remaining: int64(b.Remaining),
				Status:    Status_UNDER_LIMIT,
				ResetTime: now + (b.Limit-int64(b.Remaining))*int64(rate),
			}

			// TODO: Feature missing: check for Duration change between item/request.

			if s != nil {
				defer func() {
					s.OnChange(ctx, r, item)
					span.AddEvent("s.OnChange()")
				}()
			}

			// If we are already at the limit
			if int64(b.Remaining) == 0 {
				overLimitCounter.Add(1)
				rl.Status = Status_OVER_LIMIT
				resp = rl
				return nil
			}

			// If requested hits takes the remainder
			if int64(b.Remaining) == r.Hits {
				b.Remaining -= float64(r.Hits)
				rl.Remaining = 0
				rl.ResetTime = now + (rl.Limit-rl.Remaining)*int64(rate)
				resp = rl
				return nil
			}

			// If requested is more than available, then return over the limit
			// without updating the bucket.
			if r.Hits > int64(b.Remaining) {
				overLimitCounter.Add(1)
				rl.Status = Status_OVER_LIMIT
				resp = rl
				return nil
			}

			// Client is only interested in retrieving the current status
			if r.Hits == 0 {
				resp = rl
				return nil
			}

			b.Remaining -= float64(r.Hits)
			rl.Remaining = int64(b.Remaining)
			rl.ResetTime = now + (rl.Limit-rl.Remaining)*int64(rate)
			resp = rl
			return nil
		}

		resp, err = leakyBucketNewItem(ctx, s, c, r)
		return err
	})

	return resp, err
}

// Called by leakyBucket() when adding a new item in the store.
func leakyBucketNewItem(ctx context.Context, s Store, c Cache, r *RateLimitReq) (*RateLimitResp, error) {
	var resp *RateLimitResp

	err := tracing.Scope(ctx, func(ctx context.Context) error {
		span := trace.SpanFromContext(ctx)
		now := MillisecondNow()
		duration := r.Duration
		rate := float64(duration) / float64(r.Limit)
		if HasBehavior(r.Behavior, Behavior_DURATION_IS_GREGORIAN) {
			n := clock.Now()
			expire, err := GregorianExpiration(n, r.Duration)
			if err != nil {
				return errors.Wrap(err, "error in GregorianExpiration")
			}
			// Set the initial duration as the remainder of time until
			// the end of the gregorian interval.
			duration = expire - (n.UnixNano() / 1000000)
		}

		// Create a new leaky bucket
		b := LeakyBucketItem{
			Remaining: float64(r.Burst - r.Hits),
			Limit:     r.Limit,
			Duration:  duration,
			UpdatedAt: now,
			Burst:     r.Burst,
		}

		rl := RateLimitResp{
			Status:    Status_UNDER_LIMIT,
			Limit:     b.Limit,
			Remaining: r.Burst - r.Hits,
			ResetTime: now + (b.Limit-(r.Burst-r.Hits))*int64(rate),
		}

		// Client could be requesting that we start with the bucket OVER_LIMIT
		if r.Hits > r.Burst {
			overLimitCounter.Add(1)
			rl.Status = Status_OVER_LIMIT
			rl.Remaining = 0
			rl.ResetTime = now + (rl.Limit-rl.Remaining)*int64(rate)
			b.Remaining = 0
		}

		item := &CacheItem{
			ExpireAt:  now + duration,
			Algorithm: r.Algorithm,
			Key:       r.HashKey(),
			Value:     &b,
		}

		c.Add(item)
		span.AddEvent("c.Add()")

		if s != nil {
			s.OnChange(ctx, r, item)
			span.AddEvent("s.OnChange()")
		}

		resp = &rl
		return nil
	})

	return resp, err
}
