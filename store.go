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

package store

import (
	"context"

	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/henderiw/store/watch"
	"k8s.io/apimachinery/pkg/runtime"
)

// Storer defines the interface for a generic storage system.
type Storer[T1 any] interface {
	// Starting the watcher manager
	Start(context.Context)

	// Stopping the watcher manager
	Stop()
	// Retrieve retrieves data for the given key from the storage
	Get(key Key, opts ...GetOption) (T1, error)

	// Retrieve retrieves data for the given key from the storage
	List(visitorFunc func(Key, T1), opts ...ListOption)

	// Retrieve retrieves data for the given key from the storage
	ListKeys(opts ...ListOption) []string

	// Len returns the # entries in the store
	Len(opts ...ListOption) int

	Apply(key Key, data T1, opts ...ApplyOption) error

	// Create data with the given key in the storage
	Create(key Key, data T1, opts ...CreateOption) error

	// Update data with the given key in the storage
	Update(key Key, data T1, opts ...UpdateOption) error

	// Update data in a concurrent way through a function
	UpdateWithKeyFn(key Key, updateFunc func(obj T1) T1)

	// Delete deletes data and key from the storage
	Delete(key Key, opts ...DeleteOption) error

	// Watch watches change
	Watch(ctx context.Context, opts ...ListOption) (watch.WatchInterface[T1], error)
}

type UnstructuredStore interface {
	// Retrieve retrieves data for the given key from the storage
	Get(key Key, opts ...GetOption) (runtime.Unstructured, error)

	// Retrieve retrieves data for the given key from the storage
	List(visitorFunc func(key Key, obj runtime.Unstructured), opts ...ListOption)

	// Retrieve retrieves data for the given key from the storage
	ListKeys(opts ...ListOption) []string

	// Len returns the # entries in the store
	Len(opts ...ListOption) int

	Apply(key Key, obj runtime.Unstructured, opts ...ApplyOption) error

	// Create data with the given key in the storage
	Create(key Key, obj runtime.Unstructured, opts ...CreateOption) error

	// Update data with the given key in the storage
	Update(key Key, obj runtime.Unstructured, opts ...UpdateOption) error

	// Update data in a concurrent way through a function
	UpdateWithKeyFn(key Key, updateFunc func(obj runtime.Unstructured, opts ...UpdateOption) runtime.Unstructured)

	// Delete deletes data and key from the storage
	Delete(key Key, opts ...DeleteOption) error

	// Watch watches change
	Watch(ctx context.Context, opts ...ListOption) (watch.WatchInterface[runtime.Unstructured], error)
}

type BranchUnstructuredStore interface {
	UnstructuredStore
}

type GetOption interface {
	// ApplyToGet applies this configuration to the given get options.
	ApplyToGet(*GetOptions)
}

var _ GetOption = &GetOptions{}

type GetOptions struct {
	Commit *object.Commit
}

func (o *GetOptions) ApplyToGet(lo *GetOptions) {
	if o.Commit != nil {
		lo.Commit = o.Commit
	}
}

// ApplyOptions applies the given get options on these options,
// and then returns itself (for convenient chaining).
func (o *GetOptions) ApplyOptions(opts []GetOption) *GetOptions {
	for _, opt := range opts {
		opt.ApplyToGet(o)
	}
	return o
}

type ListOption interface {
	// ApplyToList applies this configuration to the given get options.
	ApplyToList(*ListOptions)
}

var _ ListOption = &ListOptions{}

type ListOptions struct {
	Commit *object.Commit
	Watch  bool
}

func (o *ListOptions) ApplyToList(lo *ListOptions) {
	if o.Commit != nil {
		lo.Commit = o.Commit
	}
}

// ApplyOptions applies the given get options on these options,
// and then returns itself (for convenient chaining).
func (o *ListOptions) ApplyOptions(opts []ListOption) *ListOptions {
	for _, opt := range opts {
		opt.ApplyToList(o)
	}
	return o
}

type ApplyOption interface {
	// ApplyToGet applies this configuration to the given get options.
	ApplyToApply(*ApplyOptions)
}

var _ ApplyOption = &ApplyOptions{}

type ApplyOptions struct {
}

func (o *ApplyOptions) ApplyToApply(lo *ApplyOptions) {
}

// ApplyOptions applies the given get options on these options,
// and then returns itself (for convenient chaining).
func (o *ApplyOptions) ApplyOptions(opts []ApplyOption) *ApplyOptions {
	for _, opt := range opts {
		opt.ApplyToApply(o)
	}
	return o
}

type CreateOption interface {
	// ApplyToGet applies this configuration to the given get options.
	ApplyToCreate(*CreateOptions)
}

var _ CreateOption = &CreateOptions{}

type CreateOptions struct {
}

func (o *CreateOptions) ApplyToCreate(lo *CreateOptions) {
}

func (o *CreateOptions) ApplyOptions(opts []CreateOption) *CreateOptions {
	for _, opt := range opts {
		opt.ApplyToCreate(o)
	}
	return o
}

type UpdateOption interface {
	// ApplyToGet applies this configuration to the given get options.
	ApplyToUpdate(*UpdateOptions)
}

var _ UpdateOption = &UpdateOptions{}

type UpdateOptions struct {
}

func (o *UpdateOptions) ApplyToUpdate(lo *UpdateOptions) {
}

func (o *UpdateOptions) ApplyOptions(opts []UpdateOption) *UpdateOptions {
	for _, opt := range opts {
		opt.ApplyToUpdate(o)
	}
	return o
}

type DeleteOption interface {
	// ApplyToGet applies this configuration to the given get options.
	ApplyToDelete(*DeleteOptions)
}

var _ DeleteOption = &DeleteOptions{}

type DeleteOptions struct {
}

func (o *DeleteOptions) ApplyToDelete(lo *DeleteOptions) {
}

func (o *DeleteOptions) ApplyOptions(opts []DeleteOption) *DeleteOptions {
	for _, opt := range opts {
		opt.ApplyToDelete(o)
	}
	return o
}

//server side
//getOpts := GetOptions{}
//getOpts.ApplyOptions(opts)

// client side -> TBD
//o := &GetOptions{BranchName: "dummy"}
