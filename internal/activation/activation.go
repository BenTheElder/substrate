// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package activation times the phases of bringing one actor up on an ateom.
//
// atelet reports the whole thing as its ateom_restore phase; this is the
// breakdown inside it. It is shared so both ateoms report on the same axes.
//
// Every Activation method tolerates a nil receiver: the steps being timed sit
// on paths shared with teardown, which has no activation to attribute them to.
package activation

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
)

const durationMetric = "ate.actor.activation.duration"

// buckets are tighter at the bottom than atelet's snapshot buckets: these
// phases are namespace and sandbox work that should be milliseconds, and
// starting at 5ms would put a healthy worker's whole breakdown in one bucket.
var buckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

// Instruments holds the activation histogram. A nil *Instruments is a valid
// no-op, so call sites need no guard.
type Instruments struct {
	duration metric.Float64Histogram
}

func NewInstruments(meter metric.Meter) (*Instruments, error) {
	duration, err := meter.Float64Histogram(
		durationMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of one phase of bringing an actor up on an ateom. This is the breakdown of what atelet reports as its ateom_restore phase."),
		metric.WithExplicitBucketBoundaries(buckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", durationMetric, err)
	}
	return &Instruments{duration: duration}, nil
}

// Activation accumulates one activation's phase timings, and the worker's
// occupancy: a duration alone says an activation was slow, a duration against
// the actor count says whether it will keep getting slower.
type Activation struct {
	operation   string
	attribution resources.ActorAttribution
	hosted      int

	start  time.Time
	phases []phase
}

// phase is one timed step of an activation.
type phase struct {
	name string
	d    time.Duration
}

// New starts timing an activation. operation is an ateattr.Operation* value.
func New(operation string, attribution resources.ActorAttribution) *Activation {
	return &Activation{operation: operation, attribution: attribution, start: time.Now()}
}

// Step times fn and records it under name. Errors pass through untouched, and
// a phase that failed part way is still recorded.
func (a *Activation) Step(name string, fn func() error) error {
	if a == nil {
		return fn()
	}
	t := time.Now()
	err := fn()
	a.Record(name, time.Since(t))
	return err
}

// Record adds a duration measured elsewhere, for steps that do not fit Step's
// shape. Repeated names accumulate, so a per-container step reports the sum.
func (a *Activation) Record(name string, d time.Duration) {
	if a == nil {
		return
	}
	for i := range a.phases {
		if a.phases[i].name == name {
			a.phases[i].d += d
			return
		}
	}
	a.phases = append(a.phases, phase{name: name, d: d})
}

// Since is Record for a step timed with a start instant rather than a closure.
func (a *Activation) Since(name string, t time.Time) { a.Record(name, time.Since(t)) }

// Timing starts a phase and returns the function that ends it, for use as
// `defer a.Timing(name)()` in a function that is entirely one phase.
func (a *Activation) Timing(name string) func() {
	t := time.Now()
	return func() { a.Record(name, time.Since(t)) }
}

// SetHosted records how many actors this worker held once this one was placed,
// counting this one.
func (a *Activation) SetHosted(n int) {
	if a == nil {
		return
	}
	a.hosted = n
}

// Attrs returns the phase timings as log attributes, for a runtime with its own
// breakdown line to append them to.
func (a *Activation) Attrs() []slog.Attr {
	if a == nil {
		return nil
	}
	attrs := make([]slog.Attr, 0, len(a.phases)+1)
	// The independent variable.
	attrs = append(attrs, slog.Int("hosted", a.hosted))
	for _, p := range a.phases {
		attrs = append(attrs, slog.Duration(p.name, p.d))
	}
	return attrs
}

// Finish emits the breakdown as both a histogram and one log line. The log line
// is not redundant: reading a histogram needs the collector pipeline up, while
// the line makes a single run readable with grep.
func (a *Activation) Finish(ctx context.Context, i *Instruments, err error) {
	if a == nil {
		return
	}
	total := time.Since(a.start)

	attrs := append([]slog.Attr{
		slog.String("operation", a.operation),
		slog.Any("actor", a.attribution.Ref),
	}, a.Attrs()...)
	attrs = append(attrs, slog.Duration(ateattr.ActivationPhaseTotal, total))
	if err != nil {
		attrs = append(attrs, slog.Bool("failed", true))
	}
	slog.LogAttrs(ctx, slog.LevelInfo, "Activation timing breakdown", attrs...)

	a.RecordMetrics(ctx, i, err, total)
}

// RecordMetrics emits just the histogram, for a runtime whose breakdown is
// logged elsewhere. total is not one of the accumulated phases.
func (a *Activation) RecordMetrics(ctx context.Context, i *Instruments, err error, total time.Duration) {
	if a == nil || i == nil || i.duration == nil {
		return
	}
	base := []attribute.KeyValue{
		ateattr.ActorOperationNameKey.String(a.operation),
		ateattr.TemplateAtespaceKey.String(a.attribution.TemplateAtespace),
		ateattr.TemplateNameKey.String(a.attribution.TemplateName),
	}
	for _, p := range append(a.phases, phase{ateattr.ActivationPhaseTotal, total}) {
		if p.d == 0 {
			continue
		}
		attrs := append(append([]attribute.KeyValue{}, base...), ateattr.ActivationPhaseKey.String(p.name))
		if err != nil && p.name == ateattr.ActivationPhaseTotal {
			attrs = append(attrs, ateattr.FailureReasonKey.String(ateattr.FailureReason(err)))
		}
		i.duration.Record(ctx, p.d.Seconds(), metric.WithAttributes(attrs...))
	}
}
