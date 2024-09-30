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

package memoryu

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/henderiw/logger/log"
	"github.com/henderiw/store"
	"github.com/henderiw/store/watch"
	"github.com/henderiw/store/watcher"
	"github.com/henderiw/store/watchermanager"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	// errors
	NotFound = "not found"
)

func NewStore() store.UnstructuredStore {
	return &mem{
		db:             map[store.Key]runtime.Unstructured{},
		watchermanager: watchermanager.New[runtime.Unstructured](64),
	}
}

type mem struct {
	m              sync.RWMutex
	db             map[store.Key]runtime.Unstructured
	watchermanager watchermanager.WatcherManager[runtime.Unstructured]
	watching       bool
}

func (r *mem) Start(ctx context.Context) {
	r.m.Lock()
	defer r.m.Unlock()
	r.watching = true
	go r.watchermanager.Start(ctx)
}

func (r *mem) Stop() {
	r.m.Lock()
	defer r.m.Unlock()
	r.watching = false
	r.watchermanager.Stop()
}

// Get return the type
func (r *mem) Get(key store.Key, opts ...store.GetOption) (runtime.Unstructured, error) {
	r.m.RLock()
	defer r.m.RUnlock()

	x, ok := r.db[key]
	if !ok {
		return nil, fmt.Errorf("%s, nsn: %s", NotFound, key.String())
	}
	return x, nil
}

func (r *mem) List(visitorFunc func(store.Key, runtime.Unstructured), opts ...store.ListOption) {
	r.m.RLock()
	defer r.m.RUnlock()

	for key, obj := range r.db {
		if visitorFunc != nil {
			visitorFunc(key, obj)
		}
	}
}

func (r *mem) ListKeys(opts ...store.ListOption) []string {
	keys := []string{}
	r.List(func(key store.Key, _ runtime.Unstructured) {
		keys = append(keys, key.Name)
	}, opts...)
	return keys
}

func (r *mem) Len(opts ...store.ListOption) int {
	r.m.RLock()
	defer r.m.RUnlock()

	return len(r.db)
}

func (r *mem) Apply(key store.Key, data runtime.Unstructured, opts ...store.ApplyOption) error {
	_, exists := r.db[key]
	r.update(key, data)
	if !exists {
		r.notifyWatcher(watch.WatchEvent[runtime.Unstructured]{
			Type:   watch.Added,
			Object: data,
		})
	} else {
		r.notifyWatcher(watch.WatchEvent[runtime.Unstructured]{
			Type:   watch.Modified,
			Object: data,
		})
	}
	return nil
}

func (r *mem) Create(key store.Key, data runtime.Unstructured, opts ...store.CreateOption) error {
	// if an error is returned the entry already exists
	if _, err := r.Get(key); err == nil {
		return fmt.Errorf("duplicate entry %v", key.String())
	}
	// update the cache before calling the callback since the cb fn will use this data
	r.update(key, data)

	// notify watchers
	r.notifyWatcher(watch.WatchEvent[runtime.Unstructured]{
		Type:   watch.Added,
		Object: data,
	})
	return nil
}

// Upsert creates or updates the entry in the cache
func (r *mem) Update(key store.Key, data runtime.Unstructured, opts ...store.UpdateOption) error {
	exists := true
	oldd, err := r.Get(key)
	if err != nil {
		exists = false
	}

	// update the cache before calling the callback since the cb fn will use this data
	r.update(key, data)

	// // notify watchers based on the fact the data got modified or not
	if exists {
		if !reflect.DeepEqual(oldd, data) {
			r.notifyWatcher(watch.WatchEvent[runtime.Unstructured]{
				Type:   watch.Modified,
				Object: data,
			})
		}
	} else {
		r.notifyWatcher(watch.WatchEvent[runtime.Unstructured]{
			Type:   watch.Added,
			Object: data,
		})
	}
	return nil
}

func (r *mem) UpdateWithKeyFn(key store.Key, updateFunc func(obj runtime.Unstructured, _ ...store.UpdateOption) runtime.Unstructured) {
	r.m.Lock()
	defer r.m.Unlock()

	obj := r.db[key]
	if updateFunc != nil {
		r.db[key] = updateFunc(obj)
	}
}

func (r *mem) update(key store.Key, newd runtime.Unstructured) {
	r.m.Lock()
	defer r.m.Unlock()
	r.db[key] = newd
}

func (r *mem) delete(key store.Key) {
	r.m.Lock()
	defer r.m.Unlock()
	delete(r.db, key)
}

// Delete deletes the entry in the cache
func (r *mem) Delete(key store.Key, opts ...store.DeleteOption) error {
	// only if an exisitng object gets deleted we
	// call the registered callbacks
	exists := true
	obj, err := r.Get(key)
	if err != nil {
		return nil
	}
	// if exists call the callback
	if exists {
		r.notifyWatcher(watch.WatchEvent[runtime.Unstructured]{
			Type:   watch.Deleted,
			Object: obj,
		})
	}
	// delete the entry to ensure the cb uses the proper data
	r.delete(key)
	return nil
}

func (r *mem) notifyWatcher(event watch.WatchEvent[runtime.Unstructured]) {
	r.m.RLock()
	defer r.m.RUnlock()
	if r.watching {
		r.watchermanager.WatchChan() <- event
	}

}

func (r *mem) Watch(ctx context.Context, opts ...store.ListOption) (watch.WatchInterface[runtime.Unstructured], error) {
	ctx, cancel := context.WithCancel(ctx)

	log := log.FromContext(ctx)
	log.Debug("watch")

	w := &watcher.WatcherU{
		Cancel:         cancel,
		ResultChannel:  make(chan watch.WatchEvent[runtime.Unstructured]),
		WatcherManager: r.watchermanager,
		New:            func() runtime.Unstructured { return &unstructured.Unstructured{} },
	}

	go w.ListAndWatch(ctx, r, opts...)

	return w, nil
}
