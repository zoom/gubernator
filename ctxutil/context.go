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

package ctxutil

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// WithTimeout calls context.WithTimeout and logs details of the
// deadline origin to the active trace.
func WithTimeout(ctx context.Context, duration time.Duration) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(duration)
	_, fn, line, _ := runtime.Caller(1)

	span := trace.SpanFromContext(ctx)
	span.AddEvent("Set context deadline", trace.WithAttributes(
		attribute.String("deadline", deadline.Format(time.RFC3339)),
		attribute.String("source", fmt.Sprintf("%s:%d", fn, line)),
	))

	return context.WithTimeout(ctx, duration)
}
