// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package manifest reads and writes ateapipb messages as YAML or JSON for people
package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"sigs.k8s.io/yaml"
)

// ErrEmpty is returned by Unmarshal for a document with no content.
var ErrEmpty = errors.New("manifest is empty")

// Unmarshal parses one YAML or JSON document into m. Unknown fields are an
// error, so typos don't silently drop configuration.
func Unmarshal(data []byte, m proto.Message) error {
	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return fmt.Errorf("invalid YAML: %w", err)
	}
	if string(bytes.TrimSpace(jsonData)) == "null" {
		return ErrEmpty
	}
	return protojson.Unmarshal(jsonData, m)
}

// MarshalJSON renders m as indented JSON with a trailing newline.
func MarshalJSON(m proto.Message) ([]byte, error) {
	b, err := protojson.Marshal(m)
	if err != nil {
		return nil, err
	}
	// re-encode, protojson output spacing is unstable
	// https://github.com/golang/protobuf/issues/1121
	var tree any
	if err := json.Unmarshal(b, &tree); err != nil {
		return nil, err
	}
	b, err = json.MarshalIndent(tree, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// MarshalYAML renders m as YAML.
func MarshalYAML(m proto.Message) ([]byte, error) {
	b, err := MarshalJSON(m)
	if err != nil {
		return nil, err
	}
	return yaml.JSONToYAML(b)
}
