package helps

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestNormalizeCodexGPT55ReplayNamespaces(t *testing.T) {
	body := []byte(`{
		"service_tier":"default",
		"input":[
			{"type":"function_call","name":"search","namespace":"mcp__exa","call_id":"call_1","arguments":"{}","metadata":{"namespace":"nested_function"}},
			{"type":"custom_tool_call","name":"terminal","namespace":"shell","call_id":"call_2","input":"pwd","metadata":{"namespace":"nested_custom"}},
			{"type":"message","namespace":"message_namespace","content":[{"type":"input_text","text":"hello","metadata":{"namespace":"nested_message"}}]},
			{"type":"additional_tools","namespace":"additional_namespace","tools":[{"type":"function","name":"send"}]},
			{"type":"function_call","name":"plain","call_id":"call_3","arguments":"{}"}
		],
		"tools":[{"type":"namespace","name":"mcp__exa","tools":[{"type":"function","name":"search","parameters":{"type":"object"}}]}],
		"metadata":{"namespace":"root_nested"}
	}`)

	out := NormalizeCodexGPT55ReplayNamespaces(body, "gpt-5.5")

	for _, path := range []string{"input.0.namespace", "input.1.namespace"} {
		if gjson.GetBytes(out, path).Exists() {
			t.Fatalf("%s should be removed: %s", path, out)
		}
	}
	wantValues := map[string]string{
		"service_tier":                         "default",
		"input.0.name":                         "search",
		"input.0.call_id":                      "call_1",
		"input.0.arguments":                    "{}",
		"input.0.metadata.namespace":           "nested_function",
		"input.1.name":                         "terminal",
		"input.1.call_id":                      "call_2",
		"input.1.input":                        "pwd",
		"input.1.metadata.namespace":           "nested_custom",
		"input.2.namespace":                    "message_namespace",
		"input.2.content.0.metadata.namespace": "nested_message",
		"input.3.namespace":                    "additional_namespace",
		"tools.0.type":                         "namespace",
		"tools.0.name":                         "mcp__exa",
		"tools.0.tools.0.name":                 "search",
		"tools.0.tools.0.parameters.type":      "object",
		"metadata.namespace":                   "root_nested",
	}
	for path, want := range wantValues {
		if got := gjson.GetBytes(out, path).String(); got != want {
			t.Fatalf("%s = %q, want %q; body=%s", path, got, want, out)
		}
	}
	if !bytes.Equal(out, NormalizeCodexGPT55ReplayNamespaces(out, "gpt-5.5")) {
		t.Fatalf("normalization is not idempotent: %s", out)
	}
}

func TestNormalizeCodexGPT55ReplayNamespacesPreservesOtherModels(t *testing.T) {
	body := []byte(`{"input":[{"type":"function_call","namespace":"keep"},{"type":"custom_tool_call","namespace":"keep_custom"}],"tools":[{"type":"namespace","name":"keep_tool"}]}`)
	models := []string{"gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna", "gpt-5.4", "gpt-5.5-codex", "client-alias"}
	for _, model := range models {
		t.Run(model, func(t *testing.T) {
			if out := NormalizeCodexGPT55ReplayNamespaces(body, model); !bytes.Equal(out, body) {
				t.Fatalf("model %s changed payload: got=%s want=%s", model, out, body)
			}
		})
	}
}

func TestNormalizeCodexGPT55ReplayNamespacesLeavesUnsupportedInputUntouched(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{name: "empty", body: nil},
		{name: "malformed", body: []byte(`{"input":[`)},
		{name: "string", body: []byte(`{"input":"not-an-array"}`)},
		{name: "object", body: []byte(`{"input":{"type":"function_call","namespace":"keep"}}`)},
		{name: "null", body: []byte(`{"input":null}`)},
		{name: "missing namespace", body: []byte(`{"input":[{"type":"function_call","name":"plain"}]}`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if out := NormalizeCodexGPT55ReplayNamespaces(tc.body, "gpt-5.5"); !bytes.Equal(out, tc.body) {
				t.Fatalf("payload changed: got=%s want=%s", out, tc.body)
			}
		})
	}
}
