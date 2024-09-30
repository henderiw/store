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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/henderiw/store"
	"github.com/henderiw/store/util.go"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"
)

func (r *gitrepo) filename(key store.Key) string {
	if key.Namespace != "" {
		return filepath.Join(r.rootPath, key.Namespace, key.Name+".yaml")
	}
	return filepath.Join(r.rootPath, key.Name+".yaml")
}

func (r *gitrepo) readFile(key store.Key) (runtime.Unstructured, error) {
	// this is adding the storage to the worktree
	if err := util.EnsureDir(r.rootPath); err != nil {
		return nil, fmt.Errorf("unable to write data dir: %s", err)
	}
	var obj runtime.Unstructured
	content, err := os.ReadFile(r.filename(key))
	if err != nil {
		return obj, err
	}
	object := map[string]any{}
	if err := yaml.Unmarshal(content, &object); err != nil {
		return obj, err
	}
	return &unstructured.Unstructured{
		Object: object,
	}, nil
}

func (r *gitrepo) exists(key store.Key) bool {
	_, err := os.Stat(r.filename(key))
	return err == nil
}

func (r *gitrepo) writeFile(key store.Key, obj runtime.Unstructured) error {
	if err := util.EnsureDir(r.rootPath); err != nil {
		return fmt.Errorf("unable to write data dir: %s", err)
	}
	b, err := yaml.Marshal(obj)
	if err != nil {
		return err
	}
	return os.WriteFile(r.filename(key), b, 0644)
}

func (r *gitrepo) deleteFile(key store.Key) error {
	return os.Remove(r.filename(key))
}

func (r *gitrepo) visitDir(visitorFunc func(store.Key, runtime.Unstructured)) error {
	if err := util.EnsureDir(r.rootPath); err != nil {
		return fmt.Errorf("unable to write data dir: %s", err)
	}
	return filepath.Walk(r.rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		// skip any non yaml file
		if !strings.HasSuffix(info.Name(), ".yaml") {
			return nil
		}
		// this is a yaml file by now
		// next step is find the key (namespace and name)
		name := filepath.Base(path)
		name = strings.TrimSuffix(name, ".yaml")
		namespace := ""
		parts := strings.Split(name, "_")
		if len(parts) > 1 {
			namespace = parts[0]
			name = parts[1]
		}
		key := store.KeyFromNSN(types.NamespacedName{
			Name:      name,
			Namespace: namespace,
		})

		newObj, err := r.readFile(key)
		if err != nil {
			return err
		}
		if visitorFunc != nil {
			visitorFunc(key, newObj)
		}

		return nil
	})
}
