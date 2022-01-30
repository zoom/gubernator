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

package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"

	"github.com/davecgh/go-spew/spew"
	guber "github.com/mailgun/gubernator/v2"
	"github.com/mailgun/gubernator/v2/ctxutil"
	"github.com/mailgun/holster/v4/clock"
	"github.com/mailgun/holster/v4/setter"
	"github.com/mailgun/holster/v4/syncutil"
	"github.com/mailgun/holster/v4/tracing"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
)

var log *logrus.Logger
var configFile, grpcAddress string
var concurrency uint64
var checksPerRequest uint64
var reqRate float64
var quiet bool

func main() {
	log = logrus.StandardLogger()
	flags := flag.NewFlagSet("gubernator", flag.ContinueOnError)
	flags.StringVar(&configFile, "config", "", "Environment config file")
	flags.StringVar(&grpcAddress, "e", "", "Gubernator GRPC endpoint address")
	flags.Uint64Var(&concurrency, "concurrency", 1, "Concurrent threads (default 1)")
	flags.Uint64Var(&checksPerRequest, "checks", 1, "Rate checks per request (default 1)")
	flags.Float64Var(&reqRate, "rate", 0, "Request rate overall, 0 = no rate limit")
	flags.BoolVar(&quiet, "q", false, "Quiet logging")
	checkErr(flags.Parse(os.Args[1:]))

	if quiet {
		log.SetLevel(logrus.ErrorLevel)
	}

	ctx, tracer, err := tracing.InitTracing(context.Background(),
		"github.com/mailgun/gubernator/v2/cmd/gubernator-cli")
	if err != nil {
		log.WithError(err).Warn("Error in tracing.InitTracing")
	}
	tracing.SetDefaultTracer(tracer)

	var client guber.V1Client
	err = tracing.Scope(ctx, func(ctx context.Context) error {
		// Print startup message.
		cmdLine := strings.Join(os.Args[1:], " ")
		logrus.WithContext(ctx).Info("Command line: " + cmdLine)

		conf, err := guber.SetupDaemonConfig(log, configFile)
		if err != nil {
			return err
		}
		setter.SetOverride(&conf.GRPCListenAddress, grpcAddress)

		if configFile == "" && grpcAddress == "" && os.Getenv("GUBER_GRPC_ADDRESS") == "" {
			return errors.New("please provide a GRPC endpoint via -e or from a config " +
				"file via -config or set the env GUBER_GRPC_ADDRESS")
		}

		err = guber.SetupTLS(conf.TLS)
		if err != nil {
			return err
		}

		log.WithContext(ctx).Infof("Connecting to '%s'...", conf.GRPCListenAddress)
		client, err = guber.DialV1Server(conf.GRPCListenAddress, conf.ClientTLS())
		return err
	})

	checkErr(err)

	// Generate a selection of rate limits with random limits.
	var rateLimits []*guber.RateLimitReq

	for i := 0; i < 2000; i++ {
		rateLimits = append(rateLimits, &guber.RateLimitReq{
			Name:      fmt.Sprintf("gubernator-cli-%d", i),
			UniqueKey: guber.RandomString(10),
			Hits:      1,
			Limit:     int64(randInt(1, 1000)),
			Duration:  int64(randInt(int(clock.Millisecond*500), int(clock.Second*6))),
			Behavior:  guber.Behavior_BATCHING,
			Algorithm: guber.Algorithm_TOKEN_BUCKET,
		})
	}

	fan := syncutil.NewFanOut(int(concurrency))
	var limiter *rate.Limiter
	if reqRate > 0 {
		l := rate.Limit(reqRate)
		limiter = rate.NewLimiter(l, 1)
	}

	// Replay requests in endless loop.
	for {
		for i := int(0); i < len(rateLimits); i += int(checksPerRequest) {
			req := &guber.GetRateLimitsReq{
				Requests: rateLimits[i:min(i+int(checksPerRequest), len(rateLimits))],
			}

			fan.Run(func(obj interface{}) error {
				req := obj.(*guber.GetRateLimitsReq)

				if reqRate > 0 {
					limiter.Wait(ctx)
				}

				sendRequest(ctx, client, req)

				return nil
			}, req)
		}
	}
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func checkErr(err error) {
	if err != nil {
		log.Fatalf(err.Error())
	}
}

func randInt(min, max int) int {
	return rand.Intn(max-min) + min
}

func sendRequest(ctx context.Context, client guber.V1Client, req *guber.GetRateLimitsReq) {
	_ = tracing.Scope(ctx, func(ctx context.Context) error {
		ctx, cancel := ctxutil.WithTimeout(ctx, clock.Millisecond*500)

		// Now hit our cluster with the rate limits
		resp, err := client.GetRateLimits(ctx, req)
		cancel()
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("Error in client.GetRateLimits")
			return nil
		}

		// Sanity checks.
		if resp == nil {
			log.WithContext(ctx).Error("Response object is unexpectedly nil")
			return nil
		}
		if resp.Responses == nil {
			log.WithContext(ctx).Error("Responses array is unexpectedly nil")
			return nil
		}

		// Check for overlimit response.
		overlimit := false

		for itemNum, resp := range resp.Responses {
			if resp.Status == guber.Status_OVER_LIMIT {
				overlimit = true
				log.WithContext(ctx).WithField("name", req.Requests[itemNum].Name).
					Info("Overlimit!")
			}
		}

		if overlimit {
			span := trace.SpanFromContext(ctx)
			span.SetAttributes(
				attribute.Bool("overlimit", true),
			)

			if !quiet {
				dumpResp := spew.Sdump(resp)
				log.WithContext(ctx).Info(dumpResp)
			}
		}

		return nil
	})
}
