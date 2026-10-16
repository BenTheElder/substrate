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
// this is protojson, plus the alternate value forms from (typehint) field
// options, e.g. memoryBytes: 2Gi
package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"

	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"
)

// ErrEmpty is returned by Unmarshal for a document with no content.
var ErrEmpty = errors.New("manifest is empty")

// bytesMax is the largest quantity that fits an int64, checked on the Quantity
// because Value wraps past it
var bytesMax = *resource.NewQuantity(math.MaxInt64, resource.BinarySI)

// Unmarshal parses one YAML or JSON document into m. Unknown fields are an
// error, so typos don't silently drop configuration.
func Unmarshal(data []byte, m proto.Message) error {
	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return fmt.Errorf("invalid YAML: %w", err)
	}
	tree, err := decode(jsonData)
	if err != nil {
		return err
	}
	if tree == nil {
		return ErrEmpty
	}
	if err := walk(tree, m.ProtoReflect().Descriptor(), "", fromQuantity); err != nil {
		return err
	}
	b, err := json.Marshal(tree)
	if err != nil {
		return err
	}
	return protojson.Unmarshal(b, m)
}

// MarshalJSON renders m as indented JSON with a trailing newline.
func MarshalJSON(m proto.Message) ([]byte, error) {
	b, err := protojson.Marshal(m)
	if err != nil {
		return nil, err
	}
	tree, err := decode(b)
	if err != nil {
		return nil, err
	}
	if err := walk(tree, m.ProtoReflect().Descriptor(), "", toQuantity); err != nil {
		return nil, err
	}
	// re-encode, protojson output spacing is unstable
	// https://github.com/golang/protobuf/issues/1121
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

// FormatQuantity renders n as a quantity with binary suffixes, e.g. 2<<30 as "2Gi"
func FormatQuantity(n int64) string {
	return resource.NewQuantity(n, resource.BinarySI).String()
}

// ParseQuantity parses a quantity string into an integer, rounding up
func ParseQuantity(s string) (int64, error) {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0, err
	}
	if q.Sign() < 0 {
		return 0, fmt.Errorf("%q must not be negative", s)
	}
	if q.Cmp(bytesMax) > 0 {
		return 0, fmt.Errorf("%q must be at most %s", s, bytesMax.String())
	}
	return q.Value(), nil
}

func decode(data []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var tree any
	if err := d.Decode(&tree); err != nil {
		return nil, err
	}
	return tree, nil
}

// converter rewrites one (typehint) field of obj
type converter func(obj map[string]any, fd protoreflect.FieldDescriptor, hint *ateapipb.TypeHint, path string) error

// walk calls convert for every (typehint) field in the JSON tree of a message
// of type md. Anything that does not match md is left for protojson to reject.
func walk(node any, md protoreflect.MessageDescriptor, path string, convert converter) error {
	obj, ok := node.(map[string]any)
	if !ok {
		return nil
	}
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		hint, _ := proto.GetExtension(fd.Options(), ateapipb.E_Typehint).(*ateapipb.TypeHint)
		if hint == nil {
			continue
		}
		if err := convert(obj, fd, hint, path); err != nil {
			return err
		}
	}
	for key, val := range obj {
		fd := fields.ByJSONName(key)
		if fd == nil {
			fd = fields.ByTextName(key)
		}
		if fd == nil {
			continue
		}
		fieldPath := join(path, key)
		switch {
		case fd.IsMap():
			if fd.MapValue().Message() == nil {
				continue
			}
			entries, _ := val.(map[string]any)
			for k, v := range entries {
				if err := walk(v, fd.MapValue().Message(), fmt.Sprintf("%s[%s]", fieldPath, k), convert); err != nil {
					return err
				}
			}
		case fd.Message() == nil:
		case fd.IsList():
			items, _ := val.([]any)
			for i, v := range items {
				if err := walk(v, fd.Message(), fmt.Sprintf("%s[%d]", fieldPath, i), convert); err != nil {
					return err
				}
			}
		default:
			if err := walk(val, fd.Message(), fieldPath, convert); err != nil {
				return err
			}
		}
	}
	return nil
}

func join(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func checkHint(hint *ateapipb.TypeHint) error {
	if hint.GetFormat() != ateapipb.TypeHintFormat_TYPE_HINT_FORMAT_QUANTITY {
		return fmt.Errorf("unsupported typehint format %v", hint.GetFormat())
	}
	if hint.GetScale() != 1 {
		return fmt.Errorf("unsupported typehint scale %d", hint.GetScale())
	}
	return nil
}

// fromQuantity rewrites a quantity string as the integer protojson expects,
// plain JSON numbers are left as they are
func fromQuantity(obj map[string]any, fd protoreflect.FieldDescriptor, hint *ateapipb.TypeHint, path string) error {
	// protojson accepts either name
	for _, key := range []string{fd.JSONName(), fd.TextName()} {
		s, ok := obj[key].(string)
		if !ok {
			continue
		}
		fieldPath := join(path, key)
		if err := checkHint(hint); err != nil {
			return fmt.Errorf("%s: %w", fieldPath, err)
		}
		n, err := ParseQuantity(s)
		if err != nil {
			return fmt.Errorf("%s: %w", fieldPath, err)
		}
		obj[key] = strconv.FormatInt(n, 10)
	}
	return nil
}

// toQuantity rewrites the integer field fd as a quantity string
func toQuantity(obj map[string]any, fd protoreflect.FieldDescriptor, hint *ateapipb.TypeHint, path string) error {
	val, ok := obj[fd.JSONName()]
	if !ok {
		return nil
	}
	fieldPath := join(path, fd.JSONName())
	if err := checkHint(hint); err != nil {
		return fmt.Errorf("%s: %w", fieldPath, err)
	}
	s, _ := scalar(val)
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("%s: %w", fieldPath, err)
	}
	obj[fd.JSONName()] = FormatQuantity(n)
	return nil
}

// scalar returns a JSON string or number as a string
func scalar(v any) (string, bool) {
	switch v := v.(type) {
	case string:
		return v, true
	case json.Number:
		return v.String(), true
	}
	return "", false
}
