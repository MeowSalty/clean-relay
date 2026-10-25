package cleaner_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"cleanrelay/internal/cleaner"
)

const metadataField = "internal_chat_message_metadata_passthrough"

func TestCleanFixture(t *testing.T) {
	body := readFixture(t, "metadata.before.json")
	want := readFixture(t, "metadata.after.json")
	original := bytes.Clone(body)
	c := cleaner.New([]string{metadataField})
	got, err := c.Clean(body)
	if err != nil {
		t.Fatal(err)
	}
	assertCounts(t, got, 8, 7, 7)
	assertJSONEqual(t, got.Body, want)
	if !bytes.Equal(body, original) {
		t.Fatal("Clean changed the fixture input")
	}
	assertIdempotent(t, c, got)
}

func TestCleanUnchanged(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		items int
	}{
		{"missing input", " \n {\"model\":\"x\",\"drop\":1} \r\n", 0},
		{"input null", `{"input":null}`, 0},
		{"input string", `{"input":"drop"}`, 0},
		{"input number", `{"input":9007199254740993}`, 0},
		{"input boolean", `{"input":true}`, 0},
		{"input object", `{"input":{"drop":1,"drop":2}}`, 0},
		{"empty root object", `{}`, 0},
		{"empty array", " { \"input\" : [ ] } \n", 0},
		{"no target", " { \"input\" : [ {\"keep\" : 1}, {}, \"drop\", 2, null ] } ", 5},
		{"only nested targets", `{"input":[{"content":[{"drop":null}],"extension":{"drop":true}}]}`, 1},
		{"root array", `[{"drop":1,"drop":2,"input":[{"drop":3}]}]`, 0},
		{"root empty array", `[]`, 0},
		{"root null", " \n null \t", 0},
		{"root string", `"{\"input\":[{\"drop\":1}]}"`, 0},
		{"root integer", `9007199254740993`, 0},
		{"root decimal", `0.123456789012345678901`, 0},
		{"root true", `true`, 0},
		{"root false", `false`, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(tt.body)
			got, err := cleaner.New([]string{"drop"}).Clean(body)
			if err != nil {
				t.Fatal(err)
			}
			assertCounts(t, got, tt.items, 0, 0)
			assertReused(t, got.Body, body)
		})
	}
}

func TestCleanEmptyRules(t *testing.T) {
	body := []byte(" \n {\"input\":[{\"drop\":null},{},7,\"text\",[1,2],null],\"n\":9007199254740993} \t")
	var zero cleaner.Cleaner
	for name, c := range map[string]*cleaner.Cleaner{
		"nil slice":   cleaner.New(nil),
		"empty slice": cleaner.New([]string{}),
		"zero value":  &zero,
	} {
		t.Run(name, func(t *testing.T) {
			got, err := c.Clean(body)
			if err != nil {
				t.Fatal(err)
			}
			assertCounts(t, got, 6, 0, 0)
			assertReused(t, got.Body, body)
		})
	}
}

func TestCleanChanges(t *testing.T) {
	tests := []struct {
		name     string
		fields   []string
		body     string
		want     string
		items    int
		modified int
		removed  int
	}{
		{
			name:   "mixed array keeps every position",
			fields: []string{"drop"},
			body:   `{"input":[{"id":1,"drop":null},"drop",9007199254740993,[{"drop":2,"drop":3}],null,true,false,{},[{"id":2}],{"id":3,"drop":{"x":1}}]}`,
			want:   `{"input":[{"id":1},"drop",9007199254740993,[{"drop":2,"drop":3}],null,true,false,{},[{"id":2}],{"id":3}]}`,
			items:  10, modified: 2, removed: 2,
		},
		{
			name:   "null targets and preexisting empty objects",
			fields: []string{"drop"},
			body:   `{"input":[{"drop":null},{},{"drop":null,"keep":null}]}`,
			want:   `{"input":[{},{},{"keep":null}]}`,
			items:  3, modified: 2, removed: 2,
		},
		{
			name:   "every target value type",
			fields: []string{"drop"},
			body:   `{"input":[{"drop":{}},{"drop":[]},{"drop":"text"},{"drop":9007199254740993},{"drop":true},{"drop":false},{"drop":null}]}`,
			want:   `{"input":[{},{},{},{},{},{},{}]}`,
			items:  7, modified: 7, removed: 7,
		},
		{
			name:   "multiple exact fields and repeated rules",
			fields: []string{"drop", "meta", "a.b", "", "drop"},
			body:   `{"drop":"root stays","input":[{"drop":1,"meta":null,"a.b":0,"":false,"Drop":2,"drop ":3,"drop_extra":4,"a":{"b":true}},{"drop":false},{}]}`,
			want:   `{"drop":"root stays","input":[{"Drop":2,"drop ":3,"drop_extra":4,"a":{"b":true}},{},{}]}`,
			items:  3, modified: 2, removed: 5,
		},
		{
			name:   "decoded keys match configured names",
			fields: []string{"drop", "中文", "😀"},
			body:   `{"\u0069nput":[{"\u0064rop":1,"\u4e2d\u6587":2,"\ud83d\ude00":3,"keep":true}]}`,
			want:   `{"input":[{"keep":true}]}`,
			items:  1, modified: 1, removed: 3,
		},
		{
			name:   "body strings arguments and deep properties survive",
			fields: []string{"drop"},
			body:   `{"drop":{"keep":true},"input":[{"drop":1,"content":[{"drop":"keep","text":"drop <>& \"drop\"\n中文"}],"arguments":"{\"drop\":9007199254740993}","deep":{"drop":null,"next":{"drop":false}}},{"text":"drop","drop":null}],"extension":{"drop":{"input":[{"drop":3}]}}}`,
			want:   `{"drop":{"keep":true},"input":[{"content":[{"drop":"keep","text":"drop <>& \"drop\"\n中文"}],"arguments":"{\"drop\":9007199254740993}","deep":{"drop":null,"next":{"drop":false}}},{"text":"drop"}],"extension":{"drop":{"input":[{"drop":3}]}}}`,
			items:  2, modified: 2, removed: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(tt.body)
			original := bytes.Clone(body)
			c := cleaner.New(tt.fields)
			got, err := c.Clean(body)
			if err != nil {
				t.Fatal(err)
			}
			assertCounts(t, got, tt.items, tt.modified, tt.removed)
			assertJSONEqual(t, got.Body, []byte(tt.want))
			if !bytes.Equal(body, original) {
				t.Fatal("Clean changed its input")
			}
			assertIdempotent(t, c, got)
		})
	}
}

func TestCleanRejectsInvalidAndAmbiguousJSON(t *testing.T) {
	tests := []struct {
		name string
		body string
		err  error
	}{
		{"empty", "", cleaner.ErrInvalidJSON},
		{"whitespace", " \r\n\t ", cleaner.ErrInvalidJSON},
		{"truncated root", `{"input":`, cleaner.ErrInvalidJSON},
		{"truncated array", `{"input":[{"drop":1}`, cleaner.ErrInvalidJSON},
		{"malformed later item", `{"input":[{"drop":1},{"keep":}]}`, cleaner.ErrInvalidJSON},
		{"deep invalid JSON", `{"input":[{"drop":1,"deep":{"x":[1,]}}]}`, cleaner.ErrInvalidJSON},
		{"invalid without input", `{"deep":{"x":}}`, cleaner.ErrInvalidJSON},
		{"malformed nonobject root", `[1,]`, cleaner.ErrInvalidJSON},
		{"trailing comma", `{"input":[],}`, cleaner.ErrInvalidJSON},
		{"missing separator", `{"input":[{} {}]}`, cleaner.ErrInvalidJSON},
		{"nonstring key", `{1:true}`, cleaner.ErrInvalidJSON},
		{"invalid string escape", `{"input":[{"drop":1,"text":"\q"}]}`, cleaner.ErrInvalidJSON},
		{"raw string control character", "{\"input\":[{\"drop\":1,\"text\":\"a\nb\"}]}", cleaner.ErrInvalidJSON},
		{"invalid UTF-8", "{\"input\":[{\"drop\":1,\"text\":\"\xff\"}]}", cleaner.ErrInvalidJSON},
		{"UTF-8 BOM", "\xef\xbb\xbf{}", cleaner.ErrInvalidJSON},
		{"leading zero", `{"n":01}`, cleaner.ErrInvalidJSON},
		{"invalid exponent", `{"input":[{"drop":1}],"n":1e}`, cleaner.ErrInvalidJSON},
		{"NaN", `NaN`, cleaner.ErrInvalidJSON},
		{"comment", `/* comment */ {}`, cleaner.ErrInvalidJSON},
		{"second object", `{"input":[{"drop":1}]} {}`, cleaner.ErrInvalidJSON},
		{"second scalar", `{"input":[{"drop":1}]} true`, cleaner.ErrInvalidJSON},
		{"second value after scalar", `null []`, cleaner.ErrInvalidJSON},
		{"trailing junk", `{"input":[{"drop":1}]} garbage`, cleaner.ErrInvalidJSON},
		{"trailing partial JSON", `{"input":[{"drop":1}]} {`, cleaner.ErrInvalidJSON},
		{"trailing non-JSON whitespace", "{}\v", cleaner.ErrInvalidJSON},
		{"root input duplicate", `{"input":[],"input":[{"drop":1}]}`, cleaner.ErrAmbiguousJSON},
		{"root input escape equivalent", `{"input":[],"\u0069nput":[]}`, cleaner.ErrAmbiguousJSON},
		{"unrelated root duplicate", `{"n":1,"n":2}`, cleaner.ErrAmbiguousJSON},
		{"empty root key duplicate", `{"":1,"":null}`, cleaner.ErrAmbiguousJSON},
		{"root escape equivalent", `{"other":1,"\u006fther":2}`, cleaner.ErrAmbiguousJSON},
		{"root Unicode equivalent", `{"中文":1,"\u4e2d\u6587":2}`, cleaner.ErrAmbiguousJSON},
		{"target duplicate", `{"input":[{"drop":1,"drop":2}]}`, cleaner.ErrAmbiguousJSON},
		{"target escape equivalent", `{"input":[{"drop":1,"\u0064rop":2}]}`, cleaner.ErrAmbiguousJSON},
		{"unrelated item duplicate", `{"input":[{"keep":1,"keep":2}]}`, cleaner.ErrAmbiguousJSON},
		{"later duplicate after deletion", `{"input":[{"drop":1},{"keep":1,"keep":2}]}`, cleaner.ErrAmbiguousJSON},
		{"last duplicate after deletion", `{"input":[{"drop":1},null,"text",{"x":1,"\u0078":2}]}`, cleaner.ErrAmbiguousJSON},
		{"surrogate pair equivalent", `{"input":[{"😀":1,"\ud83d\ude00":2}]}`, cleaner.ErrAmbiguousJSON},
	}
	var zero cleaner.Cleaner
	for name, c := range map[string]*cleaner.Cleaner{
		"configured":  cleaner.New([]string{"drop"}),
		"nil rules":   cleaner.New(nil),
		"empty rules": cleaner.New([]string{}),
		"zero value":  &zero,
	} {
		t.Run(name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					body := []byte(tt.body)
					original := bytes.Clone(body)
					got, err := c.Clean(body)
					if !errors.Is(err, tt.err) {
						t.Fatalf("error = %v; want errors.Is(_, %v)", err, tt.err)
					}
					other := cleaner.ErrInvalidJSON
					if tt.err == other {
						other = cleaner.ErrAmbiguousJSON
					}
					if errors.Is(err, other) {
						t.Fatalf("error also matches unrelated sentinel %v: %v", other, err)
					}
					if !reflect.DeepEqual(got, cleaner.Result{}) {
						t.Fatalf("error returned a partial result: %+v", got)
					}
					if !bytes.Equal(body, original) {
						t.Fatal("error path changed its input")
					}
				})
			}
		})
	}
}

func TestCleanThousandItems(t *testing.T) {
	const count = 1000
	items := make([]json.RawMessage, count)
	wantItems := make([]json.RawMessage, count)
	for i := range count {
		wantItems[i] = json.RawMessage(fmt.Sprintf(`{"index":%d,"text":"item-%d"}`, i, i))
		items[i] = wantItems[i]
		if i == 0 || i == count/2 || i == count-1 {
			items[i] = json.RawMessage(fmt.Sprintf(`{"index":%d,"text":"item-%d","drop":{"position":%d}}`, i, i, i))
		}
	}
	encoded, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	wantEncoded, err := json.Marshal(wantItems)
	if err != nil {
		t.Fatal(err)
	}
	body := append([]byte(`{"input":`), encoded...)
	body = append(body, '}')
	want := append([]byte(`{"input":`), wantEncoded...)
	want = append(want, '}')
	original := bytes.Clone(body)
	c := cleaner.New([]string{"drop"})
	got, err := c.Clean(body)
	if err != nil {
		t.Fatal(err)
	}
	assertCounts(t, got, count, 3, 3)
	assertJSONEqual(t, got.Body, want)
	if !bytes.Equal(body, original) {
		t.Fatal("Clean changed the long array input")
	}
	assertIdempotent(t, c, got)
}

func TestCleanPreservesNumericTokens(t *testing.T) {
	numbers := []string{
		"9007199254740993",
		"-9007199254740993",
		"1234567890123456789012345678901234567890123456789012345678901234567890",
		"0.123456789012345678901234567890123456789012345678901234567890",
		"-0.000000000000000000000000000000000000000000000000000000001234567890",
		"1e100000",
		"-1E-100000",
		"1.230000000000000000000e+0042",
		"-0",
	}
	for _, number := range numbers {
		t.Run(number, func(t *testing.T) {
			body := []byte(fmt.Sprintf(`{"root":%s,"extension":{"n":%s},"input":[{"drop":1,"n":%s,"deep":[%s]},%s,{"n":%s}]}`, number, number, number, number, number, number))
			want := []byte(fmt.Sprintf(`{"root":%s,"extension":{"n":%s},"input":[{"n":%s,"deep":[%s]},%s,{"n":%s}]}`, number, number, number, number, number, number))
			c := cleaner.New([]string{"drop"})
			got, err := c.Clean(body)
			if err != nil {
				t.Fatal(err)
			}
			assertCounts(t, got, 3, 1, 1)
			assertJSONEqual(t, got.Body, want)
			root := rawObject(t, got.Body)
			items := rawArray(t, root["input"])
			for path, value := range map[string]json.RawMessage{
				"root":             root["root"],
				"extension.n":      rawObject(t, root["extension"])["n"],
				"input[0].n":       rawObject(t, items[0])["n"],
				"input[0].deep[0]": rawArray(t, rawObject(t, items[0])["deep"])[0],
				"input[1]":         items[1],
				"input[2].n":       rawObject(t, items[2])["n"],
			} {
				if string(value) != number {
					t.Errorf("%s number = %s; want exact token %s", path, value, number)
				}
			}
			assertIdempotent(t, c, got)
		})
	}
}

func TestCleanPreservesDeeperDuplicateKeys(t *testing.T) {
	body := []byte(`{"input":[{"drop":{"x":1,"x":2},"deep":{"drop":1,"drop":2,"n":9007199254740993},"content":[{"x":1,"\u0078":2}]},{"deep":{"keep":true,"keep":false}},[{"drop":1,"drop":2}]],"extension":{"k":1,"\u006b":2}}`)
	want := []byte(`{"input":[{"deep":{"drop":1,"drop":2,"n":9007199254740993},"content":[{"x":1,"\u0078":2}]},{"deep":{"keep":true,"keep":false}},[{"drop":1,"drop":2}]],"extension":{"k":1,"\u006b":2}}`)
	c := cleaner.New([]string{"drop"})
	got, err := c.Clean(body)
	if err != nil {
		t.Fatal(err)
	}
	assertCounts(t, got, 3, 1, 1)
	assertJSONEqual(t, got.Body, want)
	beforeRoot := rawObject(t, body)
	afterRoot := rawObject(t, got.Body)
	beforeItems := rawArray(t, beforeRoot["input"])
	afterItems := rawArray(t, afterRoot["input"])
	beforeItem := rawObject(t, beforeItems[0])
	afterItem := rawObject(t, afterItems[0])
	// Comparing raw subtrees also catches silent overwriting of duplicate keys;
	// decoding these subtrees into maps would conceal that loss.
	assertRawEqual(t, afterRoot["extension"], beforeRoot["extension"])
	assertRawEqual(t, afterItem["deep"], beforeItem["deep"])
	assertRawEqual(t, afterItem["content"], beforeItem["content"])
	assertRawEqual(t, afterItems[1], beforeItems[1])
	assertRawEqual(t, afterItems[2], beforeItems[2])
	assertIdempotent(t, c, got)
}

func TestNewCopiesFieldSet(t *testing.T) {
	fields := make([]string, 1, 2)
	fields[0] = "drop"
	c := cleaner.New(fields)
	fields[0] = "keep"
	fields = append(fields, "other")
	got, err := c.Clean([]byte(`{"input":[{"drop":null,"keep":1,"other":2}]}`))
	if err != nil {
		t.Fatal(err)
	}
	assertCounts(t, got, 1, 1, 1)
	assertJSONEqual(t, got.Body, []byte(`{"input":[{"keep":1,"other":2}]}`))
}

func TestCleanOwnsChangedOutput(t *testing.T) {
	text := `{"input":[{"drop":null,"keep":9007199254740993}]}`
	storage := bytes.Repeat([]byte{0xa5}, len(text)+64)
	copy(storage, text)
	body := storage[:len(text)]
	original := bytes.Clone(storage)
	c := cleaner.New([]string{"drop"})
	first, err := c.Clean(body)
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Clean(body)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, first.Body, []byte(`{"input":[{"keep":9007199254740993}]}`))
	if &first.Body[0] == &body[0] || &second.Body[0] == &body[0] {
		t.Fatal("changed output reused the caller's backing array")
	}
	secondOriginal := bytes.Clone(second.Body)
	for i := range first.Body {
		first.Body[i] = 'x'
	}
	if !bytes.Equal(storage, original) {
		t.Fatal("Clean or changing its output overwrote input or spare capacity")
	}
	if !bytes.Equal(second.Body, secondOriginal) {
		t.Fatal("separate Clean calls share a mutable output buffer")
	}
}

func TestCleanConcurrent(t *testing.T) {
	tests := []struct {
		body     []byte
		want     []byte
		items    int
		modified int
		removed  int
		err      error
	}{
		{body: readFixture(t, "metadata.before.json"), want: readFixture(t, "metadata.after.json"), items: 8, modified: 7, removed: 7},
		{body: []byte(`{"input":[{"drop":1,"n":9007199254740993},null,{}]}`), want: []byte(`{"input":[{"n":9007199254740993},null,{}]}`), items: 3, modified: 1, removed: 1},
		{body: []byte(" \n {\"input\":[{\"keep\":null}]} \t"), want: []byte(" \n {\"input\":[{\"keep\":null}]} \t"), items: 1},
		{body: []byte(`{"input":[{"drop":1},{"k":1,"k":2}]}`), err: cleaner.ErrAmbiguousJSON},
		{body: []byte(`{"input":[{"drop":1}]} {}`), err: cleaner.ErrInvalidJSON},
	}
	originals := make([][]byte, len(tests))
	expected := make([]any, len(tests))
	for i, tt := range tests {
		originals[i] = bytes.Clone(tt.body)
		if tt.err == nil {
			var err error
			expected[i], err = decodeJSON(tt.want)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	c := cleaner.New([]string{metadataField, "drop"})
	const workers, iterations = 32, 40
	failures := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			for iteration := range iterations {
				index := (worker + iteration) % len(tests)
				tt := tests[index]
				got, err := c.Clean(tt.body)
				if tt.err != nil {
					if !errors.Is(err, tt.err) || !reflect.DeepEqual(got, cleaner.Result{}) {
						failures <- fmt.Errorf("worker %d: error = %v, result = %+v; want zero result and %v", worker, err, got, tt.err)
						return
					}
					continue
				}
				if err != nil {
					failures <- fmt.Errorf("worker %d: %w", worker, err)
					return
				}
				value, decodeErr := decodeJSON(got.Body)
				if decodeErr != nil || !reflect.DeepEqual(value, expected[index]) || got.InputItems != tt.items || got.ModifiedItems != tt.modified || got.RemovedFields != tt.removed {
					failures <- fmt.Errorf("worker %d: unexpected result %+v, decode error %v", worker, got, decodeErr)
					return
				}
				if tt.removed == 0 {
					if !bytes.Equal(got.Body, tt.body) || &got.Body[0] != &tt.body[0] {
						failures <- fmt.Errorf("worker %d: unchanged body was not reused", worker)
						return
					}
				} else {
					// Each changed body belongs to this call, even while other
					// calls are reading the same input and generating output.
					got.Body[0] = 'x'
				}
			}
		})
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	for i, tt := range tests {
		if !bytes.Equal(tt.body, originals[i]) {
			t.Errorf("concurrent calls changed shared input %d", i)
		}
	}
}

func FuzzClean(f *testing.F) {
	for _, seed := range []string{
		`{"input":[{"drop":1,"n":9007199254740993},null,{}]}`,
		`{"\u0069nput":[{"drop":null,"deep":{"k":1,"k":2}}]}`,
		`{"input":[{"drop":1},{"k":1,"\u006b":2}]}`,
		`{"input":[{"drop":1}]} {}`,
		`[1,"text",null,{"drop":1,"drop":2}]`,
		"\xff",
		"",
	} {
		f.Add([]byte(seed))
	}
	c := cleaner.New([]string{"drop"})
	f.Fuzz(func(t *testing.T, body []byte) {
		original := bytes.Clone(body)
		got, err := c.Clean(body)
		if !bytes.Equal(body, original) {
			t.Fatal("Clean changed its input")
		}
		if err != nil {
			if !errors.Is(err, cleaner.ErrInvalidJSON) && !errors.Is(err, cleaner.ErrAmbiguousJSON) {
				t.Fatalf("unclassified error: %v", err)
			}
			if !reflect.DeepEqual(got, cleaner.Result{}) {
				t.Fatalf("partial result on error: %+v", got)
			}
			return
		}
		if _, err := decodeJSON(got.Body); err != nil {
			t.Fatalf("output is not valid complete JSON: %v", err)
		}
		if got.InputItems < got.ModifiedItems || got.ModifiedItems > got.RemovedFields || got.ModifiedItems < 0 {
			t.Fatalf("inconsistent counts: %+v", got)
		}
		if got.RemovedFields == 0 {
			assertReused(t, got.Body, body)
		}
		assertIdempotent(t, c, got)
	})
}

func rawObject(t *testing.T, body []byte) map[string]json.RawMessage {
	t.Helper()
	var object map[string]json.RawMessage
	if err := json.Unmarshal(body, &object); err != nil {
		t.Fatal(err)
	}
	return object
}

func rawArray(t *testing.T, body []byte) []json.RawMessage {
	t.Helper()
	var items []json.RawMessage
	if err := json.Unmarshal(body, &items); err != nil {
		t.Fatal(err)
	}
	return items
}

func assertRawEqual(t *testing.T, got, want []byte) {
	t.Helper()
	var gotCompact, wantCompact bytes.Buffer
	if err := json.Compact(&gotCompact, got); err != nil {
		t.Fatal(err)
	}
	if err := json.Compact(&wantCompact, want); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotCompact.Bytes(), wantCompact.Bytes()) {
		t.Fatalf("raw subtree differs:\ngot:  %s\nwant: %s", got, want)
	}
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "testdata", "requests", name))
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// decodeJSON preserves every numeric token as json.Number, including values
// beyond float64 precision or range. It also verifies the entire output is JSON.
func decodeJSON(body []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("expected EOF after JSON value, got %v", err)
	}
	return value, nil
}

func assertJSONEqual(t *testing.T, got, want []byte) {
	t.Helper()
	gotValue, err := decodeJSON(got)
	if err != nil {
		t.Fatalf("output is not complete valid JSON: %v", err)
	}
	wantValue, err := decodeJSON(want)
	if err != nil {
		t.Fatalf("expected JSON is invalid: %v", err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("JSON differs:\ngot:  %s\nwant: %s", got, want)
	}
}

func assertCounts(t *testing.T, got cleaner.Result, items, modified, removed int) {
	t.Helper()
	if got.InputItems != items || got.ModifiedItems != modified || got.RemovedFields != removed {
		t.Fatalf("counts = %d/%d/%d; want %d/%d/%d", got.InputItems, got.ModifiedItems, got.RemovedFields, items, modified, removed)
	}
}

func assertReused(t *testing.T, got, body []byte) {
	t.Helper()
	if !bytes.Equal(got, body) {
		t.Fatalf("unchanged bytes differ:\ngot:  %s\nwant: %s", got, body)
	}
	if len(got) == 0 || &got[0] != &body[0] {
		t.Fatal("unchanged output did not reuse the input byte slice")
	}
}

func assertIdempotent(t *testing.T, c *cleaner.Cleaner, first cleaner.Result) {
	t.Helper()
	second, err := c.Clean(first.Body)
	if err != nil {
		t.Fatalf("second cleaning: %v", err)
	}
	assertCounts(t, second, first.InputItems, 0, 0)
	assertReused(t, second.Body, first.Body)
}
