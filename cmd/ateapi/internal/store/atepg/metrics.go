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

package atepg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

const (
	poolConnectionsMetric = "ate.store.pool.connections"
	poolAcquiresMetric    = "ate.store.pool.acquires"
	poolAcquireWaitMetric = "ate.store.pool.acquire.wait"
)

// RegisterPoolMetrics reports what the connection pools are doing, which is
// otherwise invisible: a query waiting for a connection and a query waiting on
// PostgreSQL look the same from above, and only the first is fixed by a larger
// pool. The waited acquires and the wait total are what tell them apart.
func (p *Persistence) RegisterPoolMetrics(meter metric.Meter) error {
	connections, err := meter.Int64ObservableUpDownCounter(
		poolConnectionsMetric,
		metric.WithUnit("{connection}"),
		metric.WithDescription("Connections held by a store pool, by state."),
	)
	if err != nil {
		return fmt.Errorf("create %s updowncounter: %w", poolConnectionsMetric, err)
	}
	acquires, err := meter.Int64ObservableCounter(
		poolAcquiresMetric,
		metric.WithUnit("{acquire}"),
		metric.WithDescription("Connection acquisitions from a store pool, by whether one was free."),
	)
	if err != nil {
		return fmt.Errorf("create %s counter: %w", poolAcquiresMetric, err)
	}
	acquireWait, err := meter.Float64ObservableCounter(
		poolAcquireWaitMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Time spent waiting for a connection from a store pool."),
	)
	if err != nil {
		return fmt.Errorf("create %s counter: %w", poolAcquireWaitMetric, err)
	}

	pools := map[string]*pgxpool.Pool{
		ateattr.StorePoolMain:  p.pool,
		ateattr.StorePoolWatch: p.watchPool,
	}
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for name, pool := range pools {
			if pool == nil {
				continue
			}
			stat := pool.Stat()
			poolAttr := attribute.NewSet(ateattr.StorePoolKey.String(name))
			o.ObserveInt64(connections, int64(stat.AcquiredConns()), metric.WithAttributeSet(
				attribute.NewSet(ateattr.StorePoolKey.String(name), ateattr.PoolConnectionStateKey.String(ateattr.PoolConnectionAcquired))))
			o.ObserveInt64(connections, int64(stat.IdleConns()), metric.WithAttributeSet(
				attribute.NewSet(ateattr.StorePoolKey.String(name), ateattr.PoolConnectionStateKey.String(ateattr.PoolConnectionIdle))))
			o.ObserveInt64(connections, int64(stat.MaxConns()), metric.WithAttributeSet(
				attribute.NewSet(ateattr.StorePoolKey.String(name), ateattr.PoolConnectionStateKey.String(ateattr.PoolConnectionMax))))

			// EmptyAcquireCount is the acquisitions that found no free
			// connection and had to wait for one, so it is the count that says
			// the pool, not PostgreSQL, is the queue.
			waited := stat.EmptyAcquireCount()
			o.ObserveInt64(acquires, waited, metric.WithAttributeSet(
				attribute.NewSet(ateattr.StorePoolKey.String(name), ateattr.AcquireOutcomeKey.String(ateattr.AcquireOutcomeWaited))))
			o.ObserveInt64(acquires, stat.AcquireCount()-waited, metric.WithAttributeSet(
				attribute.NewSet(ateattr.StorePoolKey.String(name), ateattr.AcquireOutcomeKey.String(ateattr.AcquireOutcomeImmediate))))

			o.ObserveFloat64(acquireWait, stat.AcquireDuration().Seconds(), metric.WithAttributeSet(poolAttr))
		}
		return nil
	}, connections, acquires, acquireWait)
	if err != nil {
		return fmt.Errorf("register store pool callback: %w", err)
	}
	return nil
}
