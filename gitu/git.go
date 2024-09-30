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

package gitu

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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// errors
	NotFound = "not found"
)

type Config struct {
	RootPath   string
	PathInRepo string
	//Repo     *git.Repository

	GroupResource schema.GroupResource
	NewFunc       func() runtime.Unstructured
}

func NewStore(cfg *Config) (store.BranchUnstructuredStore, error) {
	rootPath := filepath.Join(cfg.RootPath, cfg.GroupResource.Group, cfg.GroupResource.Resource)

	// this is adding the storage to the worktree
	if err := util.EnsureDir(rootPath); err != nil {
		return nil, fmt.Errorf("unable to write data dir: %s", err)
	}
	// WHEN Do we create a branch
	return &gitrepo{
		//repo:     cfg.Repo,
		rootPath:       rootPath,
		relRepoPath:    filepath.Join(cfg.PathInRepo, "db", cfg.GroupResource.Group, cfg.GroupResource.Resource),
		newFunc:        cfg.NewFunc,
		watchermanager: watchermanager.New[runtime.Unstructured](64),
	}, nil
}

type gitrepo struct {
	
	//repo     *git.Repository
	rootPath       string
	relRepoPath    string
	newFunc        func() runtime.Unstructured
	watchermanager watchermanager.WatcherManager[runtime.Unstructured]
	m sync.RWMutex
	watching       bool
}

func (r *gitrepo) Start(ctx context.Context) {
	r.m.Lock()
	defer r.m.Unlock()
	r.watching = true
	go r.watchermanager.Start(ctx)
}

func (r *gitrepo) Stop() {
	r.m.Lock()
	defer r.m.Unlock()
	r.watching = false
	r.watchermanager.Stop()
}

// Get return the type
func (r *gitrepo) Get(key store.Key, opts ...store.GetOption) (runtime.Unstructured, error) {
	o := store.GetOptions{}
	o.ApplyOptions(opts)
	if o.Commit != nil {
		return r.readFileFromCommit(key, o.Commit)
	}
	return r.readFile(key)
}

func (r *gitrepo) List(visitorFunc func(store.Key, runtime.Unstructured), opts ...store.ListOption) {
	o := store.ListOptions{}
	o.ApplyOptions(opts)
	if o.Commit != nil {
		if err := r.visitCommitTree(o.Commit, visitorFunc); err != nil {
			log := log.FromContext(context.Background())
			log.Error("cannot list visiting dir failed", "error", err.Error())
		}
		return
	}
	if err := r.visitDir(visitorFunc); err != nil {
		log := log.FromContext(context.Background())
		log.Error("cannot list visiting dir failed", "error", err.Error())
	}
}

func (r *gitrepo) ListKeys(opts ...store.ListOption) []string {
	keys := []string{}
	r.List(func(key store.Key, _ runtime.Unstructured) {
		keys = append(keys, key.Name)
	}, opts...)
	return keys
}

func (r *gitrepo) Len(opts ...store.ListOption) int {
	items := 0
	r.List(func(key store.Key, _ runtime.Unstructured) {
		items++
	}, opts...)
	return items
}

func (r *gitrepo) Apply(key store.Key, data runtime.Unstructured, opts ...store.ApplyOption) error {
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

func (r *gitrepo) Create(key store.Key, data runtime.Unstructured, opts ...store.CreateOption) error {
	// if an error is returned the entry already exists
	if _, err := r.Get(key); err == nil {
		return fmt.Errorf("duplicate entry %v", key.String())
	}
	// update the store before calling the callback since the cb fn will use this data
	if err := r.update(key, data); err != nil {
		return err
	}

	// notify watchers
	r.notifyWatcher(watch.WatchEvent[runtime.Unstructured]{
		Type:   watch.Added,
		Object: data,
	})
	return nil
}

// Upsert creates or updates the entry in the cache
func (r *gitrepo) Update(key store.Key, data runtime.Unstructured, opts ...store.UpdateOption) error {
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

func (r *gitrepo) UpdateWithKeyFn(key store.Key, updateFunc func(obj runtime.Unstructured, _ ...store.UpdateOption) runtime.Unstructured) {
	obj, _ := r.readFile(key)
	if updateFunc != nil {
		obj = updateFunc(obj)
		r.update(key, obj)
	}
}

func (r *gitrepo) update(key store.Key, newd runtime.Unstructured) error {
	return r.writeFile(key, newd)
}

func (r *gitrepo) delete(key store.Key) error {
	return r.deleteFile(key)
}

// Delete deletes the entry in the cache
func (r *gitrepo) Delete(key store.Key, opts ...store.DeleteOption) error {
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

func (r *gitrepo) notifyWatcher(event watch.WatchEvent[runtime.Unstructured]) {
	r.m.RLock()
	defer r.m.RUnlock()
	if r.watching {
		r.watchermanager.WatchChan() <- event
	}
}

func (r *gitrepo) Watch(ctx context.Context, opts ...store.ListOption) (watch.WatchInterface[runtime.Unstructured], error) {
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
