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

package file

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/henderiw/store"
	"github.com/henderiw/store/util.go"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func (r *file) filename(key store.Key) string {
	if key.Namespace != "" {
		return filepath.Join(r.objRootPath, key.Namespace, key.Name+".json")
	}
	return filepath.Join(r.objRootPath, key.Name+".json")
}

func (r *file) readFile(key store.Key) (runtime.Object, error) {
	var obj runtime.Object
	content, err := os.ReadFile(r.filename(key))
	if err != nil {
		return obj, err
	}
	newObj := r.newFunc()
	decodeObj, _, err := r.codec.Decode(content, nil, newObj)
	if err != nil {
		return obj, err
	}
	return decodeObj, nil
}

func convert(obj any) (runtime.Object, error) {
	runtimeObj, ok := obj.(runtime.Object)
	if !ok {
		return nil, fmt.Errorf("unsupported type: %v", reflect.TypeOf(obj).Name())
	}
	return runtimeObj, nil
}

func (r *file) exists(key store.Key) bool {
	_, err := os.Stat(r.filename(key))
	return err == nil
}

func (r *file) writeFile(key store.Key, obj runtime.Object) error {
	runtimeObj, err := convert(obj)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	if err := r.codec.Encode(runtimeObj, buf); err != nil {
		return err
	}
	if err := util.EnsureDir(filepath.Dir(r.filename(key))); err != nil {
		return err
	}
	return os.WriteFile(r.filename(key), buf.Bytes(), 0644)
}

func (r *file) deleteFile(key store.Key) error {
	return os.Remove(r.filename(key))
}

func (r *file) visitDir(visitorFunc func(store.Key, runtime.Object), _ ...store.ListOption) error {
	return filepath.Walk(r.objRootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		// skip any non json file
		if !strings.HasSuffix(info.Name(), ".json") {
			return nil
		}
		// this is a json file by now
		// next step is find the key (namespace and name)
		name := strings.TrimSuffix(filepath.Base(path), ".json")
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
