/*
Copyright 2024 Nokia.

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

package watchermanager

import (
	"github.com/henderiw/store"
	"github.com/henderiw/store/watch"
)

type Watcher[T1 any] interface {
	OnChange(watch.WatchEvent[T1]) bool
}

type watcher[T1 any] struct {
	key string // uuid allocated to allow for delete
	// isDone should return non-nil when the watcher is finished.
	// This is normally bound to ctx.Err()
	isDone        func() error
	callback      Watcher[T1]        // interface that handles OnChange
	filterOptions *store.ListOptions // TODO update this
}
