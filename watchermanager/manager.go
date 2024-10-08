// Copyright 2023 The xxx Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package watchermanager

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/henderiw/logger/log"
	"github.com/henderiw/store"
	"github.com/henderiw/store/watch"
	"golang.org/x/sync/semaphore"
)

type WatcherManager[T1 any] interface {
	// start the generic watcher channel
	Start(ctx context.Context)
	Stop()
	WatchChan() chan watch.WatchEvent[T1]
	Add(ctx context.Context, callback Watcher[T1], opts ...store.ListOption) error // Del is handled with the isDone or callBackFn result
}

func New[T1 any](maxWatchers int64) WatcherManager[T1] {
	return &watcherManager[T1]{
		sem:      semaphore.NewWeighted(maxWatchers),
		watchers: newWatchersCache[T1](),
		watchCh:  make(chan watch.WatchEvent[T1]),
	}
}

type watcherManager[T1 any] struct {
	sem      *semaphore.Weighted
	watchers *watchers[T1]
	watchCh  chan watch.WatchEvent[T1]
	cancel   context.CancelFunc
}

func (r *watcherManager[T1]) WatchChan() chan watch.WatchEvent[T1] {
	return r.watchCh
}

// Add adds a watcher to the watcherManager and allocates a uuid per watcher to make the delete
// easier, the uuid is used only internally
func (r *watcherManager[T1]) Add(ctx context.Context, callback Watcher[T1], opts ...store.ListOption) error {
	log := log.FromContext(ctx)
	o := &store.ListOptions{}
	o.ApplyOptions(opts)

	// see if we have to clean done watcher
	for _, w := range r.watchers.list() {
		if err := w.isDone(); err != nil {
			r.watchers.del(w.key)
			r.sem.Release(1)
		}
	}

	ok := r.sem.TryAcquire(1)
	if !ok {
		return fmt.Errorf("max number of watchers reached")
	}
	// allocate uuid for the watcher
	uuid := uuid.New().String()
	// initialize the watcher
	w := &watcher[T1]{
		key:           uuid,
		isDone:        ctx.Err, // handles watcher stop and deletion gracefully
		callback:      callback,
		filterOptions: o,
	}
	r.watchers.add(uuid, w)
	log.Debug("watchers", "total", r.watchers.len())
	return nil
}

// Start is a blocking function that handles Change events from a server implementation
// and sends them to the watchers it is managing
// The events are send via callback fn in a concurrent waitGroup to handle concurrent operation
// when an error or the callback signals the delete
func (r *watcherManager[T1]) Start(ctx context.Context) {
	ctx, r.cancel = context.WithCancel(ctx)
	log := log.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-r.watchCh:
			log.Debug("watchermanager event received", "eventType", event.Type, "watchers", r.watchers.len())
			var wg sync.WaitGroup
			for _, w := range r.watchers.list() {
				w := w
				log.Debug("watcher event processing", "eventType", event.Type, "key", w.key)
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := w.isDone(); err != nil {
						log.Debug("stopping watcher due to error", "key", w.key)
						r.watchers.del(w.key)
						r.sem.Release(1)
						return
					}

					// the callback deals with filtering
					if keepGoing := w.callback.OnChange(watch.WatchEvent[T1]{
						Type:   event.Type,
						Object: event.Object,
					}); !keepGoing {
						log.Debug("stopping watcher due to !keepGoing", "key", w.key)
						r.watchers.del(w.key)
						r.sem.Release(1)
						return
					}
					log.Debug("watch callback done", "key", w.key)
				}()
			}
			log.Debug("watchermanager goroutines waiting", "eventType", event.Type, "watchers", r.watchers.len())
			wg.Wait()
			log.Debug("watchermanager goroutines done waiting", "eventType", event.Type, "watchers", r.watchers.len())
		}
	}
}

func (r *watcherManager[T1]) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
}
