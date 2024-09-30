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

package memory

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
	//metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
)

const (
	// errors
	NotFound = "not found"
)

func NewStore[T1 any](new func() T1) store.Storer[T1] {
	return &mem[T1]{
		db:             map[store.Key]T1{},
		watchermanager: watchermanager.New[T1](64),
		new:            new,
	}
}

type mem[T1 any] struct {
	m              sync.RWMutex
	db             map[store.Key]T1
	watchermanager watchermanager.WatcherManager[T1]
	new            func() T1
	watching       bool
}

func (r *mem[T1]) Start(ctx context.Context) {
	r.m.Lock()
	defer r.m.Unlock()
	r.watching = true
	go r.watchermanager.Start(ctx)
}

func (r *mem[T1]) Stop() {
	r.m.Lock()
	defer r.m.Unlock()
	r.watching = false
	r.watchermanager.Stop()
}

// Get return the type
func (r *mem[T1]) Get(key store.Key, opts ...store.GetOption) (T1, error) {
	r.m.RLock()
	defer r.m.RUnlock()

	x, ok := r.db[key]
	if !ok {
		return *new(T1), fmt.Errorf("%s, nsn: %s", NotFound, key.String())
	}
	return x, nil
}

func (r *mem[T1]) List(visitorFunc func(key store.Key, obj T1), _ ...store.ListOption) {
	r.m.RLock()
	defer r.m.RUnlock()

	for key, obj := range r.db {
		if visitorFunc != nil {
			visitorFunc(key, obj)
		}
	}
}

func (r *mem[T1]) ListKeys(opts ...store.ListOption) []string {
	keys := []string{}
	r.List(func(key store.Key, _ T1) {
		keys = append(keys, key.Name)
	}, opts...)
	return keys
}

func (r *mem[T1]) Len(_ ...store.ListOption) int {
	r.m.RLock()
	defer r.m.RUnlock()

	return len(r.db)
}

func (r *mem[T1]) Apply(key store.Key, data T1, opts ...store.ApplyOption) error {
	//_, exists := r.db[key]
	exists := true
	if _, err := r.Get(key); err != nil {
		exists = false
	}
	r.update(key, data)
	if !exists {
		r.notifyWatcher(watch.WatchEvent[T1]{
			Type:   watch.Added,
			Object: data,
		})
	} else {
		r.notifyWatcher(watch.WatchEvent[T1]{
			Type:   watch.Modified,
			Object: data,
		})
	}
	return nil
}

func (r *mem[T1]) Create(key store.Key, data T1, opts ...store.CreateOption) error {
	// if an error is returned the entry already exists
	if _, err := r.Get(key); err == nil {
		return fmt.Errorf("duplicate entry %v", key.String())
	}
	// update the cache before calling the callback since the cb fn will use this data
	r.update(key, data)

	// notify watchers
	r.notifyWatcher(watch.WatchEvent[T1]{
		Type:   watch.Added,
		Object: data,
	})
	return nil
}

// Upsert creates or updates the entry in the cache
func (r *mem[T1]) Update(key store.Key, data T1, opts ...store.UpdateOption) error {
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
			r.notifyWatcher(watch.WatchEvent[T1]{
				Type:   watch.Modified,
				Object: data,
			})
		}
	} else {
		r.notifyWatcher(watch.WatchEvent[T1]{
			Type:   watch.Added,
			Object: data,
		})
	}
	return nil
}

func (r *mem[T1]) UpdateWithKeyFn(key store.Key, updateFunc func(obj T1) T1) {
	r.m.Lock()
	defer r.m.Unlock()

	obj := r.db[key]
	if updateFunc != nil {
		r.db[key] = updateFunc(obj)
	}
}

func (r *mem[T1]) update(key store.Key, newd T1) {
	r.m.Lock()
	defer r.m.Unlock()
	r.db[key] = newd
}

func (r *mem[T1]) delete(key store.Key) {
	r.m.Lock()
	defer r.m.Unlock()
	delete(r.db, key)
}

// Delete deletes the entry in the cache
func (r *mem[T1]) Delete(key store.Key, _ ...store.DeleteOption) error {
	// only if an exisitng object gets deleted we
	// call the registered callbacks
	obj, err := r.Get(key)
	if err != nil {
		return nil
	}
	// delete the entry to ensure the cb uses the proper data
	r.delete(key)
	// if exists call the callback
	r.notifyWatcher(watch.WatchEvent[T1]{
		Type:   watch.Deleted,
		Object: obj,
	})

	return nil
}

func (r *mem[T1]) notifyWatcher(event watch.WatchEvent[T1]) {
	r.m.RLock()
	defer r.m.RUnlock()
	if r.watching {
		r.watchermanager.WatchChan() <- event
	}
}

func (r *mem[T1]) Watch(ctx context.Context, opts ...store.ListOption) (watch.WatchInterface[T1], error) {
	ctx, cancel := context.WithCancel(ctx)

	log := log.FromContext(ctx)
	log.Debug("watch")

	w := &watcher.Watcher[T1]{
		Cancel:         cancel,
		ResultChannel:  make(chan watch.WatchEvent[T1]),
		WatcherManager: r.watchermanager,
		New:            r.new,
	}

	go w.ListAndWatch(ctx, r, opts...)

	return w, nil
}
