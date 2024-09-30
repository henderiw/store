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

package watcher

import (
	"context"
	"fmt"
	"sync"

	"github.com/henderiw/logger/log"
	"github.com/henderiw/store"
	"github.com/henderiw/store/watch"
	"github.com/henderiw/store/watchermanager"
	"k8s.io/apimachinery/pkg/runtime"
)

// implements the watchermanager Watcher interface
// implenents the k8s watch.Interface interface
type WatcherU struct {
	// interfce to the observer
	Cancel         func()
	ResultChannel  chan watch.WatchEvent[runtime.Unstructured]
	WatcherManager watchermanager.WatcherManager[runtime.Unstructured]
	New            func() runtime.Unstructured

	// protection against concurrent access
	m             sync.Mutex
	eventCallback func(eventType watch.WatchEvent[runtime.Unstructured]) bool
	done          bool
}

var _ watch.WatchInterface[any] = &Watcher[any]{}

// Stop stops watching. Will close the channel returned by ResultChan(). Releases
// any resources used by the watch.
func (r *WatcherU) Stop() {
	r.Cancel()
}

// ResultChan returns a chan which will receive all the events. If an error occurs
// or Stop() is called, the implementation will close this channel and
// release any resources used by the watch.
func (r *WatcherU) ResultChan() <-chan watch.WatchEvent[runtime.Unstructured] {
	return r.ResultChannel
}

// Implement the watcchermanafer.Watcher interface
// OnChange is the callback called when a object changes.
func (r *WatcherU) OnChange(eventType watch.WatchEvent[runtime.Unstructured]) bool {
	r.m.Lock()
	defer r.m.Unlock()

	return r.eventCallback(eventType)
}

func (r *WatcherU) ListAndWatch(ctx context.Context, store store.UnstructuredStore, opts ...store.ListOption) {
	log := log.FromContext(ctx)
	if err := r.innerListAndWatch(ctx, store, opts...); err != nil {
		// TODO: We need to populate the object on this error
		// Most likely happens when we cancel a context, stop a watch
		log.Debug("sending error to watch stream", "error", err)
		ev := watch.WatchEvent[runtime.Unstructured]{
			Type:   watch.Error,
			Object: r.New(),
		}
		r.ResultChannel <- ev
	}

	log.Debug("stop listAndWatch")
	r.Stop()
}

// innerListAndWatch provides the callback handler
// 1. add a callback handler to receive any event we get while collecting the list of existing resources
// 2.
func (r *WatcherU) innerListAndWatch(ctx context.Context, s store.UnstructuredStore, opts ...store.ListOption) error {
	log := log.FromContext(ctx)

	o := &store.ListOptions{}
	o.ApplyOptions(opts)

	errorResult := make(chan error)

	// backlog logs the events during startup
	var backlog []watch.WatchEvent[runtime.Unstructured]
	// Make sure we hold the lock when setting the eventCallback, as it
	// will be read by other goroutines when events happen.
	r.m.Lock()
	r.eventCallback = func(event watch.WatchEvent[runtime.Unstructured]) bool {
		if r.done {
			return false
		}
		backlog = append(backlog, event)
		return true
	}
	r.m.Unlock()

	// we add the watcher to the watchermanager and start building a backlog for intermediate changes
	// while we startup, the backlog will be replayed once synced
	log.Debug("starting watch")

	if err := r.WatcherManager.Add(ctx, r, opts...); err != nil {
		return err
	}

	// options.Watch means watch only no listing
	if !o.Watch {
		log.Debug("starting list watch")

		s.List(func(k store.Key, t runtime.Unstructured) {
			ev := watch.WatchEvent[runtime.Unstructured]{
				Type:   watch.Added,
				Object: t,
			}
			r.sendWatchEvent(ctx, ev)
		})

		log.Debug("finished list watch")
	} else {
		log.Debug("watch only, no list")
	}

	// Repeatedly flush the backlog until we catch up
	for {
		r.m.Lock()
		chunk := backlog
		backlog = nil
		r.m.Unlock()

		if len(chunk) == 0 {
			break
		}
		log.Debug("flushing backlog", "chunk length", len(chunk))
		for _, ev := range chunk {
			r.sendWatchEvent(ctx, ev)
		}
	}

	r.m.Lock()
	// Pick up anything that squeezed in
	for _, ev := range backlog {
		r.sendWatchEvent(ctx, ev)
	}

	log.Debug("moving into streaming mode")
	r.eventCallback = func(event watch.WatchEvent[runtime.Unstructured]) bool {
		//accessor, _ := meta.Accessor(obj)
		log.Debug("eventCallBack", "eventType", event.Type, "nsn", fmt.Sprintf("%v", event.Object))
		if r.done {
			return false
		}
		ev := event
		r.sendWatchEvent(ctx, ev)
		return true
	}
	r.m.Unlock()

	select {
	case <-ctx.Done():
		r.setDone()
		return ctx.Err()

	case err := <-errorResult:
		r.setDone()
		return err
	}
}

func (r *WatcherU) sendWatchEvent(ctx context.Context, event watch.WatchEvent[runtime.Unstructured]) {
	// TODO: Handle the case that the watch channel is full?

	log := log.FromContext(ctx).With("event", event.Type)
	log.Debug("sending watch event")

	r.ResultChannel <- event
}

func (r *WatcherU) setDone() {
	r.m.Lock()
	defer r.m.Unlock()
	r.done = true
}
