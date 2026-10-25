// Package cleaner removes configured fields from direct objects in a root input array.
package cleaner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

var (
	// ErrInvalidJSON identifies malformed JSON or content after the root value.
	ErrInvalidJSON = errors.New("invalid JSON")
	// ErrAmbiguousJSON identifies duplicate keys in objects needed for cleaning.
	ErrAmbiguousJSON = errors.New("ambiguous JSON")
)

// Result contains the complete body and counts for a successful cleaning.
type Result struct {
	Body          []byte
	InputItems    int
	ModifiedItems int
	RemovedFields int
}

// Cleaner holds an immutable field set and is safe for concurrent calls.
// The zero value validates JSON without removing fields.
type Cleaner struct {
	fields map[string]struct{}
}

// New copies fields into an immutable set. Names match decoded JSON keys exactly.
func New(fields []string) *Cleaner {
	set := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		set[field] = struct{}{}
	}
	return &Cleaner{fields: set}
}

// Clean validates one complete JSON value and removes configured direct fields
// from objects in the root input array. It never writes to body. When nothing is
// removed, Result.Body shares body's backing array. Errors return a zero Result.
func (c *Cleaner) Clean(body []byte) (Result, error) {
	document, err := decodeDocument(body)
	if err != nil {
		return Result{}, err
	}

	result := Result{Body: body}
	if document[0] != '{' {
		return result, nil
	}
	root, err := decodeObject(document)
	if err != nil {
		return Result{}, fmt.Errorf("root object: %w", err)
	}
	input, ok := root["input"]
	if !ok || input[0] != '[' {
		return result, nil
	}
	var items []json.RawMessage
	if err := json.NewDecoder(bytes.NewReader(input)).Decode(&items); err != nil {
		return Result{}, fmt.Errorf("%w: decode input: %w", ErrInvalidJSON, err)
	}
	result.InputItems = len(items)
	for i, item := range items {
		if item[0] != '{' {
			continue
		}
		object, err := decodeObject(item)
		if err != nil {
			return Result{}, fmt.Errorf("input[%d]: %w", i, err)
		}
		removed := 0
		for key := range object {
			if _, ok := c.fields[key]; ok {
				delete(object, key)
				removed++
			}
		}
		if removed == 0 {
			continue
		}
		encoded, err := json.Marshal(object)
		if err != nil {
			return Result{}, fmt.Errorf("%w: encode input[%d]: %w", ErrInvalidJSON, i, err)
		}
		items[i] = encoded
		result.ModifiedItems++
		result.RemovedFields += removed
	}
	if result.RemovedFields == 0 {
		return result, nil
	}
	encodedInput, err := json.Marshal(items)
	if err != nil {
		return Result{}, fmt.Errorf("%w: encode input: %w", ErrInvalidJSON, err)
	}
	root["input"] = encodedInput
	encodedBody, err := json.Marshal(root)
	if err != nil {
		return Result{}, fmt.Errorf("%w: encode body: %w", ErrInvalidJSON, err)
	}
	result.Body = encodedBody
	return result, nil
}

func decodeDocument(body []byte) (json.RawMessage, error) {
	if !utf8.Valid(body) {
		return nil, fmt.Errorf("%w: body is not UTF-8", ErrInvalidJSON)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	var document json.RawMessage
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("%w: decode body: %w", ErrInvalidJSON, err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: trailing content", ErrInvalidJSON)
	}
	return document, nil
}

// decodeObject expands an already validated object by one level. Token decodes
// key escapes before comparison; RawMessage leaves values, including numbers
// and any deeper duplicate keys, intact.
func decodeObject(body json.RawMessage) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("%w: open object: %w", ErrInvalidJSON, err)
	}
	object := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("%w: read object key: %w", ErrInvalidJSON, err)
		}
		key := token.(string) // The validated object requires a string key here.
		if _, exists := object[key]; exists {
			return nil, fmt.Errorf("%w: duplicate key %q", ErrAmbiguousJSON, key)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("%w: read object value: %w", ErrInvalidJSON, err)
		}
		object[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("%w: close object: %w", ErrInvalidJSON, err)
	}
	return object, nil
}
