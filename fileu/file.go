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

package fileu

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/henderiw/logger/log"
	"github.com/henderiw/store"
	"github.com/henderiw/store/util.go"
	"github.com/henderiw/store/watch"
	"github.com/henderiw/store/watcher"
	"github.com/henderiw/store/watchermanager"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// errors
	NotFound = "not found"
)

type Config struct {
	GroupResource schema.GroupResource
	RootPath      string
	NewFunc       func() runtime.Unstructured
}

func NewStore(cfg *Config) (store.UnstructuredStore, error) {
	objRootPath := filepath.Join(cfg.RootPath, cfg.GroupResource.Group, cfg.GroupResource.Resource)
	if err := util.EnsureDir(objRootPath); err != nil {
		return nil, fmt.Errorf("unable to write data dir: %s", err)
	}
	return &file{
		//grPrefix:    fmt.Sprintf("%s_%s", cfg.GroupResource.Group, cfg.GroupResource.Resource),
		objRootPath:    objRootPath,
		newFunc:        cfg.NewFunc,
		watchermanager: watchermanager.New[runtime.Unstructured](64),
	}, nil
}

type file struct {
	//grPrefix    string
	objRootPath    string
	newFunc        func() runtime.Unstructured
	watchermanager watchermanager.WatcherManager[runtime.Unstructured]
	m              sync.RWMutex
	watching       bool
}

func (r *file) Start(ctx context.Context) {
	r.m.Lock()
	defer r.m.Unlock()
	r.watching = true
	go r.watchermanager.Start(ctx)
}

func (r *file) Stop() {
	r.m.Lock()
	defer r.m.Unlock()
	r.watching = false
	r.watchermanager.Stop()
}

// Get return the type
func (r *file) Get(key store.Key, opts ...store.GetOption) (runtime.Unstructured, error) {
	//o := store.GetOptions{}
	//o.ApplyOptions(opts)

	return r.readFile(key)
}

func (r *file) List(visitorFunc func(store.Key, runtime.Unstructured), opts ...store.ListOption) {
	log := log.FromContext(context.Background())
	if err := r.visitDir(visitorFunc); err != nil {
		log.Error("cannot list visiting dir failed", "error", err.Error())
	}
}

func (r *file) ListKeys(opts ...store.ListOption) []string {
	keys := []string{}
	r.List(func(key store.Key, _ runtime.Unstructured) {
		keys = append(keys, key.Name)
	}, opts...)
	return keys
}

func (r *file) Len(opts ...store.ListOption) int {
	items := 0
	r.List(func(key store.Key, _ runtime.Unstructured) {
		items++
	}, opts...)
	return items
}

func (r *file) Apply(key store.Key, data runtime.Unstructured, opts ...store.ApplyOption) error {
	exists := r.exists(key)
	if err := r.update(key, data); err != nil {
		return err
	}
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

func (r *file) Create(key store.Key, data runtime.Unstructured, opts ...store.CreateOption) error {
	// if an error is returned the entry already exists
	if _, err := r.Get(key); err == nil {
		return fmt.Errorf("duplicate entry %v", key.String())
	}
	// update the store before calling the callback since the cb fn will use this data
	if err := r.update(key, data); err != nil {
		return err
	}

	r.notifyWatcher(watch.WatchEvent[runtime.Unstructured]{
		Type:   watch.Added,
		Object: data,
	})
	return nil
}

// Upsert creates or updates the entry in the cache
func (r *file) Update(key store.Key, data runtime.Unstructured, opts ...store.UpdateOption) error {
	exists := true
	oldd, err := r.Get(key)
	if err != nil {
		exists = false
	}

	// update the cache before calling the callback since the cb fn will use this data
	if err := r.update(key, data); err != nil {
		return err
	}

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

func (r *file) UpdateWithKeyFn(key store.Key, updateFunc func(obj runtime.Unstructured, _ ...store.UpdateOption) runtime.Unstructured) {
	obj, _ := r.readFile(key)
	if updateFunc != nil {
		obj = updateFunc(obj)
		r.update(key, obj)
	}
}

func (r *file) update(key store.Key, newd runtime.Unstructured) error {
	return r.writeFile(key, newd)
}

func (r *file) delete(key store.Key) error {
	return r.deleteFile(key)
}

// Delete deletes the entry in the cache
func (r *file) Delete(key store.Key, opts ...store.DeleteOption) error {
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
	return r.delete(key)
}

func (r *file) notifyWatcher(event watch.WatchEvent[runtime.Unstructured]) {
	r.m.RLock()
	defer r.m.RUnlock()
	if r.watching {
		r.watchermanager.WatchChan() <- event
	}
}

func (r *file) Watch(ctx context.Context, opts ...store.ListOption) (watch.WatchInterface[runtime.Unstructured], error) {
	ctx, cancel := context.WithCancel(ctx)

	log := log.FromContext(ctx)
	log.Debug("watch")

	w := &watcher.WatcherU{
		Cancel:         cancel,
		ResultChannel:  make(chan watch.WatchEvent[runtime.Unstructured]),
		WatcherManager: r.watchermanager,
		New:            r.newFunc,
	}

	go w.ListAndWatch(ctx, r, opts...)

	return w, nil
}
