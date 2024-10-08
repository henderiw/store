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
	"os"
	"path/filepath"
	"strings"

	"github.com/henderiw/store"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"
)

func (r *file) filename(key store.Key) string {
	if key.Namespace != "" {
		return filepath.Join(r.objRootPath, key.Namespace, key.Name+".yaml")
	}
	return filepath.Join(r.objRootPath, key.Name+".yaml")
}

func (r *file) readFile(key store.Key) (runtime.Unstructured, error) {
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

func (r *file) exists(key store.Key) bool {
	_, err := os.Stat(r.filename(key))
	return err == nil
}

func (r *file) writeFile(key store.Key, obj runtime.Unstructured) error {
	b, err := yaml.Marshal(obj)
	if err != nil {
		return err
	}
	return os.WriteFile(r.filename(key), b, 0644)
}

func (r *file) deleteFile(key store.Key) error {
	return os.Remove(r.filename(key))
}

func (r *file) visitDir(visitorFunc func(store.Key, runtime.Unstructured), _ ...store.ListOption) error {
	return filepath.Walk(r.objRootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		// skip any non json file
		if !strings.HasSuffix(info.Name(), ".yaml") {
			return nil
		}
		// skip if the group resource prefix does not match
		//if !strings.HasPrefix(info.Name(), r.grPrefix) {
		//	return nil
		//}
		// this is a yaml file by now
		// next step is find the key (namespace and name)
		name := strings.TrimSuffix(filepath.Base(path), ".yaml")
		namespace := ""
		pathSplit := strings.Split(path, "/")
		if len(pathSplit) > (len(strings.Split(r.objRootPath, "/")) + 1) {
			namespace = pathSplit[len(pathSplit)-2]
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
