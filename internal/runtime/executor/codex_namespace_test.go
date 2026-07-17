package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexGPT55NamespaceNormalizationHTTPPaths(t *testing.T) {
	tests := []struct {
		name        string
		model       string
		stream      bool
		serviceTier string
	}{
		{name: "non-stream", model: "gpt-5.5"},
		{name: "stream with suffix", model: "gpt-5.5(high)", stream: true, serviceTier: "priority"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			captured := make(chan []byte, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				captured <- bytes.Clone(body)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"output\":[],\"usage\":{\"input_tokens\":0,\"output_tokens\":0,\"total_tokens\":0}}}\n\n"))
			}))
			defer server.Close()

			payload := []byte(`{"model":"client-alias","input":[{"type":"function_call","name":"search","namespace":"mcp__exa","arguments":"{}","metadata":{"namespace":"nested"}}],"tools":[{"type":"namespace","name":"mcp__exa","tools":[{"type":"function","name":"search"}]}]}`)
			if tc.serviceTier != "" {
				payload = []byte(`{"model":"client-alias","service_tier":"` + tc.serviceTier + `","input":[{"type":"function_call","name":"search","namespace":"mcp__exa","arguments":"{}","metadata":{"namespace":"nested"}}],"tools":[{"type":"namespace","name":"mcp__exa","tools":[{"type":"function","name":"search"}]}]}`)
			}
			exec := NewCodexExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
			auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
			req := cliproxyexecutor.Request{Model: tc.model, Payload: payload}
			opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response"), Stream: tc.stream}

			if tc.stream {
				result, err := exec.ExecuteStream(context.Background(), auth, req, opts)
				if err != nil {
					t.Fatalf("ExecuteStream() error = %v", err)
				}
				for range result.Chunks {
				}
			} else if _, err := exec.Execute(context.Background(), auth, req, opts); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}

			select {
			case body := <-captured:
				assertCodexGPT55NormalizedPayload(t, body)
				if got := gjson.GetBytes(body, "model").String(); got != "gpt-5.5" {
					t.Fatalf("model = %q, want final upstream model gpt-5.5; body=%s", got, body)
				}
				tier := gjson.GetBytes(body, "service_tier")
				if tc.serviceTier == "" && tier.Exists() {
					t.Fatalf("service_tier was added by default: %s", body)
				}
				if tc.serviceTier != "" && tier.String() != tc.serviceTier {
					t.Fatalf("service_tier = %q, want %q; body=%s", tier.String(), tc.serviceTier, body)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for HTTP upstream payload")
			}
		})
	}
}

func TestCodexGPT55NamespaceNormalizationWebsocketPath(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	captured := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()

		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Errorf("read websocket request: %v", errRead)
			return
		}
		captured <- bytes.Clone(payload)
		completed := []byte(`{"type":"response.completed","response":{"id":"resp_1","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Errorf("write websocket completion: %v", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5.5",
		Payload: []byte(`{"model":"gpt-5.5","input":[{"type":"custom_tool_call","name":"terminal","namespace":"shell","input":"pwd","metadata":{"namespace":"nested"}}],"tools":[{"type":"namespace","name":"shell","tools":[{"type":"custom","name":"terminal"}]}]}`),
	}
	if _, err := exec.Execute(context.Background(), auth, req, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex")}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	select {
	case body := <-captured:
		if gjson.GetBytes(body, "input.0.namespace").Exists() {
			t.Fatalf("custom_tool_call namespace was not removed: %s", body)
		}
		if got := gjson.GetBytes(body, "input.0.metadata.namespace").String(); got != "nested" {
			t.Fatalf("nested namespace = %q, want nested; body=%s", got, body)
		}
		if got := gjson.GetBytes(body, "tools.0.type").String(); got != "namespace" {
			t.Fatalf("namespace tool type = %q, want namespace; body=%s", got, body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for websocket upstream payload")
	}
}

func TestCodexGPT55NamespaceNormalizationTokenCountPath(t *testing.T) {
	exec := NewCodexExecutor(&config.Config{})
	body, baseModel, err := exec.prepareCodexTokenCountBody(cliproxyexecutor.Request{
		Model:   "gpt-5.5",
		Payload: []byte(`{"model":"client-alias","input":[{"type":"function_call","name":"search","namespace":"mcp__exa","arguments":"{}","metadata":{"namespace":"nested"}}],"tools":[{"type":"namespace","name":"mcp__exa"}]}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response")})
	if err != nil {
		t.Fatalf("prepareCodexTokenCountBody() error = %v", err)
	}
	if baseModel != "gpt-5.5" {
		t.Fatalf("base model = %q, want gpt-5.5", baseModel)
	}
	assertCodexGPT55NormalizedPayload(t, body)
	if _, err = exec.CountTokens(context.Background(), nil, cliproxyexecutor.Request{
		Model:   "gpt-5.5",
		Payload: []byte(`{"model":"client-alias","input":[{"type":"function_call","name":"search","namespace":"mcp__exa","arguments":"{}"}]}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response")}); err != nil {
		t.Fatalf("CountTokens() error = %v", err)
	}
}

func TestCodexGPT55NamespaceNormalizationCredentialRetry(t *testing.T) {
	firstBodies := make(chan []byte, 1)
	firstServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		firstBodies <- bytes.Clone(body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"type":"rate_limit_error","message":"retry"}}`))
	}))
	defer firstServer.Close()

	secondBodies := make(chan []byte, 1)
	secondServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		secondBodies <- bytes.Clone(body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"output\":[],\"usage\":{\"input_tokens\":0,\"output_tokens\":0,\"total_tokens\":0}}}\n\n"))
	}))
	defer secondServer.Close()

	exec := NewCodexExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	req := cliproxyexecutor.Request{
		Model:   "gpt-5.5",
		Payload: []byte(`{"model":"gpt-5.5","input":[{"type":"function_call","name":"search","namespace":"mcp__exa","arguments":"{}","metadata":{"namespace":"nested"}}],"tools":[{"type":"namespace","name":"mcp__exa"}]}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response")}
	firstAuth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "first", "base_url": firstServer.URL}}
	if _, err := exec.Execute(context.Background(), firstAuth, req, opts); err == nil {
		t.Fatal("first Execute() error = nil, want retryable upstream error")
	}
	if !gjson.GetBytes(req.Payload, "input.0.namespace").Exists() {
		t.Fatal("normalization mutated the reusable original request")
	}

	secondAuth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "second", "base_url": secondServer.URL}}
	if _, err := exec.Execute(context.Background(), secondAuth, req, opts); err != nil {
		t.Fatalf("second Execute() error = %v", err)
	}

	for label, bodies := range map[string]<-chan []byte{"first attempt": firstBodies, "credential retry": secondBodies} {
		select {
		case body := <-bodies:
			assertCodexGPT55NormalizedPayload(t, body)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s payload", label)
		}
	}
}

func assertCodexGPT55NormalizedPayload(t *testing.T, body []byte) {
	t.Helper()
	if gjson.GetBytes(body, "input.0.namespace").Exists() {
		t.Fatalf("replay namespace was not removed: %s", body)
	}
	if got := gjson.GetBytes(body, "input.0.metadata.namespace").String(); got != "nested" {
		t.Fatalf("nested namespace = %q, want nested; body=%s", got, body)
	}
	if got := gjson.GetBytes(body, "tools.0.type").String(); got != "namespace" {
		t.Fatalf("namespace tool type = %q, want namespace; body=%s", got, body)
	}
}
