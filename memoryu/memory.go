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

package memoryu_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/henderiw/logger/log"
	"github.com/henderiw/store"
	"github.com/henderiw/store/watch"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	// errors
	NotFound = "not found"
)

func NewStore() store.UnstructuredStore {
	return &mem{
		db:       map[store.Key]runtime.Unstructured{},
		watchers: watch.NewWatchers[runtime.Unstructured](64),
	}
}

type mem struct {
	m        sync.RWMutex
	db       map[store.Key]runtime.Unstructured
	watchers *watch.Watchers[runtime.Unstructured]
}

// Get return the type
func (r *mem) Get(ctx context.Context, key store.Key) (runtime.Unstructured, error) {
	r.m.RLock()
	defer r.m.RUnlock()

	x, ok := r.db[key]
	if !ok {
		return nil, fmt.Errorf("%s, nsn: %s", NotFound, key.String())
	}
	return x, nil
}

func (r *mem) List(ctx context.Context, visitorFunc func(ctx context.Context, key store.Key, obj runtime.Unstructured)) {
	r.m.RLock()
	defer r.m.RUnlock()

	for key, obj := range r.db {
		if visitorFunc != nil {
			visitorFunc(ctx, key, obj)
		}
	}
}

func (r *mem) ListKeys(ctx context.Context) []string {
	keys := []string{}
	r.List(ctx, func(ctx context.Context, key store.Key, _ runtime.Unstructured) {
		keys = append(keys, key.Name)
	})
	return keys
}

func (r *mem) Len(ctx context.Context) int {
	r.m.RLock()
	defer r.m.RUnlock()

	return len(r.db)
}

func (r *mem) Create(ctx context.Context, key store.Key, data runtime.Unstructured) error {
	// if an error is returned the entry already exists
	if _, err := r.Get(ctx, key); err == nil {
		return fmt.Errorf("duplicate entry %v", key.String())
	}
	// update the cache before calling the callback since the cb fn will use this data
	r.update(ctx, key, data)

	// notify watchers
	r.watchers.NotifyWatchers(ctx, watch.Event[runtime.Unstructured]{
		Type:   watch.Added,
		Object: data,
	})
	return nil
}

// Upsert creates or updates the entry in the cache
func (r *mem) Update(ctx context.Context, key store.Key, data runtime.Unstructured) error {
	exists := true
	oldd, err := r.Get(ctx, key)
	if err != nil {
		exists = false
	}

	// update the cache before calling the callback since the cb fn will use this data
	r.update(ctx, key, data)

	// // notify watchers based on the fact the data got modified or not
	if exists {
		if !reflect.DeepEqual(oldd, data) {
			r.watchers.NotifyWatchers(ctx, watch.Event[runtime.Unstructured]{
				Type:   watch.Modified,
				Object: data,
			})
		}
	} else {
		r.watchers.NotifyWatchers(ctx, watch.Event[runtime.Unstructured]{
			Type:   watch.Added,
			Object: data,
		})
	}
	return nil
}

func (r *mem) UpdateWithKeyFn(ctx context.Context, key store.Key, updateFunc func(ctx context.Context, obj runtime.Unstructured) runtime.Unstructured) {
	r.m.Lock()
	defer r.m.Unlock()

	obj := r.db[key]
	if updateFunc != nil {
		r.db[key] = updateFunc(ctx, obj)
	}
}

func (r *mem) update(_ context.Context, key store.Key, newd runtime.Unstructured) {
	r.m.Lock()
	defer r.m.Unlock()
	r.db[key] = newd
}

func (r *mem) delete(_ context.Context, key store.Key) {
	r.m.Lock()
	defer r.m.Unlock()
	delete(r.db, key)
}

// Delete deletes the entry in the cache
func (r *mem) Delete(ctx context.Context, key store.Key) error {
	// only if an exisitng object gets deleted we
	// call the registered callbacks
	exists := true
	obj, err := r.Get(ctx, key)
	if err != nil {
		return nil
	}
	// if exists call the callback
	if exists {
		r.watchers.NotifyWatchers(ctx, watch.Event[runtime.Unstructured]{
			Type:   watch.Deleted,
			Object: obj,
		})
	}
	// delete the entry to ensure the cb uses the proper data
	r.delete(ctx, key)
	return nil
}

func (r *mem) Watch(ctx context.Context) (watch.Interface[runtime.Unstructured], error) {
	//r.m.Lock()
	//defer r.m.Unlock()

	log := log.FromContext(ctx)
	log.Debug("watch memory store")
	if r.watchers.IsExhausted() {
		return nil, fmt.Errorf("cannot allocate watcher, out of resources")
	}
	w := r.watchers.GetWatchContext()

	// On initial watch, send all the existing objects
	items := map[store.Key]runtime.Unstructured{}
	r.List(ctx, func(ctx context.Context, key store.Key, obj runtime.Unstructured) {
		items[key] = obj
	})
	log.Debug("watch list items", "len", len(items))
	for _, obj := range items {
		w.ResultCh <- watch.Event[runtime.Unstructured]{
			Type:   watch.Added,
			Object: obj,
		}
	}
	// this ensures the initial events from the list
	// get processed first
	log.Debug("watcher add")
	if err := r.watchers.Add(w); err != nil {
		log.Debug("cannot add watcher", "error", err.Error())
		return nil, err
	}
	log.Debug("watcher added")
	return w, nil
}
