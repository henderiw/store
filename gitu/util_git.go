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
	"io"
	"path/filepath"
	"strings"

	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/henderiw/logger/log"
	"github.com/henderiw/store"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"
)

func (r *gitrepo) readFileFromCommit(key store.Key, commit *object.Commit) (runtime.Unstructured, error) {
	var obj runtime.Unstructured
	// Retrieve the file from the commit
	file, err := commit.File(r.filename(key))
	if err != nil {
		return obj, fmt.Errorf("failed to find file in commit %v", err)
	}

	// Read the contents of the file
	reader, err := file.Reader()
	if err != nil {
		return obj, fmt.Errorf("failed to open file reader %v", err)
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		return obj, fmt.Errorf("failed to read file content %v", err)
	}
	object := map[string]any{}
	if err := yaml.Unmarshal(content, &object); err != nil {
		return obj, fmt.Errorf("failed to unmarshal file content %v", err)
	}
	return &unstructured.Unstructured{
		Object: object,
	}, nil
}

func (r *gitrepo) visitCommitTree(commit *object.Commit, visitorFunc func(store.Key, runtime.Unstructured)) error {
	log := log.FromContext(context.Background())
	// Get the tree from the commit
	tree, err := commit.Tree()
	if err != nil {
		return fmt.Errorf("failed to get tree from commit err: %v", err)
	}

	// Get the subtree for the specific directory
	subtree, err := tree.Tree(r.relRepoPath)
	if err != nil {
		// this can happen -> git does not keep track of an empty directory
		log.Debug("failed to get subtree for directory", "directory", r.rootPath, "relpath", r.relRepoPath, "commit", commit.Hash.String(), "error", err)
		return nil
	}
	// List files in the subtree
	err = subtree.Files().ForEach(func(f *object.File) error {
		// skip any non yaml file
		if !strings.HasSuffix(f.Name, ".yaml") {
			return nil
		}
		name := filepath.Base(f.Name)
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

		content, err := f.Contents()
		if err != nil {
			return fmt.Errorf("failed toget file content %v", err)
		}
		object := map[string]any{}
		if err := yaml.Unmarshal([]byte(content), &object); err != nil {
			return fmt.Errorf("failed to unmarshal file content %v", err)
		}
		newObj := &unstructured.Unstructured{
			Object: object,
		}

		if visitorFunc != nil {
			visitorFunc(key, newObj)
		}
		return nil
	})
	return err
}
