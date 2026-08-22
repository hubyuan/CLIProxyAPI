package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

type attemptFailurePolicyFunc func(context.Context, pluginapi.AttemptFailureRequest) (pluginapi.AttemptFailureResponse, bool, error)

func (f attemptFailurePolicyFunc) DecideAttemptFailure(ctx context.Context, req pluginapi.AttemptFailureRequest) (pluginapi.AttemptFailureResponse, bool, error) {
	return f(ctx, req)
}

type attemptFailureExecutor struct {
	mu         sync.Mutex
	identifier string
	execute    func(*Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error)
	stream     func(*Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error)
	calls      []string
}

func (e *attemptFailureExecutor) Identifier() string { return e.identifier }
func (e *attemptFailureExecutor) Execute(_ context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.mu.Lock()
	e.calls = append(e.calls, auth.ID)
	e.mu.Unlock()
	return e.execute(auth, req, opts)
}
func (e *attemptFailureExecutor) ExecuteStream(_ context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls = append(e.calls, auth.ID)
	e.mu.Unlock()
	return e.stream(auth, req, opts)
}
func (e *attemptFailureExecutor) Refresh(context.Context, *Auth) (*Auth, error) {
	return nil, errors.New("not implemented")
}
func (e *attemptFailureExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}
func (e *attemptFailureExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *attemptFailureExecutor) callIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.calls...)
}

func registerAttemptFailureAuth(t *testing.T, manager *Manager, id, provider, model string) *Auth {
	t.Helper()
	auth := &Auth{ID: id, Provider: provider, Status: StatusActive}
	registry.GetGlobalRegistry().RegisterClient(id, provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	return auth
}

func transient429(message string) error {
	return testAttemptStatusError{code: http.StatusTooManyRequests, msg: message}
}

type testAttemptStatusError struct {
	code int
	msg  string
}

func (e testAttemptStatusError) Error() string   { return e.msg }
func (e testAttemptStatusError) StatusCode() int { return e.code }

func TestSameAuthRetry_NonStreamingSuccess(t *testing.T) {
	m := NewManager(nil, nil, nil)
	auth := registerAttemptFailureAuth(t, m, "codex-retry-auth", "codex", "gpt-5")
	registerAttemptFailureAuth(t, m, "codex-retry-other", "codex", "gpt-5")
	auth.Attributes = map[string]string{"priority": "10"}
	if _, err := m.Update(context.Background(), auth); err != nil {
		t.Fatalf("update auth priority: %v", err)
	}
	calls := 0
	exec := &attemptFailureExecutor{identifier: "codex"}
	exec.execute = func(_ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
		calls++
		if calls == 1 {
			return cliproxyexecutor.Response{}, transient429(`{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"try again"}}`)
		}
		return cliproxyexecutor.Response{Payload: []byte("ok")}, nil
	}
	m.RegisterExecutor(exec)
	m.SetPluginAttemptFailurePolicy(attemptFailurePolicyFunc(func(_ context.Context, req pluginapi.AttemptFailureRequest) (pluginapi.AttemptFailureResponse, bool, error) {
		if req.Provider != "codex" || req.RetryCount != 0 {
			t.Errorf("unexpected request: %#v", req)
		}
		return pluginapi.AttemptFailureResponse{RetrySameAuth: true}, true, nil
	}))
	if _, err := m.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: "gpt-5"}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
	if got := exec.callIDs(); len(got) != 2 || got[0] != auth.ID || got[1] != auth.ID {
		t.Fatalf("auth calls = %#v, want same auth", got)
	}
}

func TestSameAuthRetry_ExhaustedIsAvailabilityNeutral(t *testing.T) {
	m := NewManager(nil, nil, nil)
	auth := registerAttemptFailureAuth(t, m, "codex-exhausted-auth", "codex", "gpt-5")
	exec := &attemptFailureExecutor{identifier: "codex"}
	exec.execute = func(_ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
		return cliproxyexecutor.Response{}, transient429(`{"error":{"type":"rate_limit_error","message":"busy"}}`)
	}
	m.RegisterExecutor(exec)
	m.SetPluginAttemptFailurePolicy(attemptFailurePolicyFunc(func(context.Context, pluginapi.AttemptFailureRequest) (pluginapi.AttemptFailureResponse, bool, error) {
		return pluginapi.AttemptFailureResponse{RetrySameAuth: true}, true, nil
	}))
	_, err := m.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: "gpt-5"}, cliproxyexecutor.Options{})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "busy") {
		t.Fatalf("Execute() error = %v, want final 429", err)
	}
	if got := len(exec.callIDs()); got != sameAuthMaxRetries+1 {
		t.Fatalf("calls = %d, want %d", got, sameAuthMaxRetries+1)
	}
	m.mu.RLock()
	state := m.auths[auth.ID]
	status := state.Status
	m.mu.RUnlock()
	if status != StatusActive {
		t.Fatalf("auth status = %v, want active", status)
	}
}

func TestSameAuthRetry_Classification(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want bool
	}{
		{name: "rate limit", msg: `{"error":{"type":"rate_limit_error"}}`, want: true},
		{name: "capacity", msg: "selected model is at capacity", want: true},
		{name: "usage limit", msg: `{"error":{"code":"usage_limit_reached"}}`, want: false},
		{name: "quota window", msg: "quota reset window reached", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, _, got := codexTransient429Kind(transient429(tt.msg))
			if got != tt.want {
				t.Fatalf("eligible = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSameAuthRetry_DelayClamped(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetPluginAttemptFailurePolicy(attemptFailurePolicyFunc(func(context.Context, pluginapi.AttemptFailureRequest) (pluginapi.AttemptFailureResponse, bool, error) {
		return pluginapi.AttemptFailureResponse{RetrySameAuth: true, Delay: time.Minute}, true, nil
	}))
	request := pluginapi.AttemptFailureRequest{Provider: "codex", StatusCode: http.StatusTooManyRequests, Remaining: time.Minute}
	response, handled, err := m.decideSameAuthRetry(context.Background(), request)
	if err != nil || !handled {
		t.Fatalf("decideSameAuthRetry() = %#v, %v, %v", response, handled, err)
	}
	if response.Delay != sameAuthMaxDelay {
		t.Fatalf("delay = %s, want %s", response.Delay, sameAuthMaxDelay)
	}
}

func TestSameAuthRetry_PluginTimeoutIsBounded(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetPluginAttemptFailurePolicy(attemptFailurePolicyFunc(func(ctx context.Context, _ pluginapi.AttemptFailureRequest) (pluginapi.AttemptFailureResponse, bool, error) {
		<-ctx.Done()
		return pluginapi.AttemptFailureResponse{}, true, ctx.Err()
	}))
	started := time.Now()
	_, handled, err := m.decideSameAuthRetry(context.Background(), pluginapi.AttemptFailureRequest{Provider: "codex", StatusCode: http.StatusTooManyRequests, Remaining: time.Second})
	if err == nil || !handled {
		t.Fatalf("decideSameAuthRetry() = handled %v, err %v, want bounded error", handled, err)
	}
	if elapsed := time.Since(started); elapsed > pluginDecisionCap+100*time.Millisecond {
		t.Fatalf("plugin decision took %s, cap %s", elapsed, pluginDecisionCap)
	}
}

func TestSameAuthRetry_StreamBootstrapThenSuccess(t *testing.T) {
	m := NewManager(nil, nil, nil)
	registerAttemptFailureAuth(t, m, "codex-stream-auth", "codex", "gpt-5")
	calls := 0
	exec := &attemptFailureExecutor{identifier: "codex"}
	exec.stream = func(_ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
		calls++
		chunks := make(chan cliproxyexecutor.StreamChunk, 1)
		if calls == 1 {
			chunks <- cliproxyexecutor.StreamChunk{Err: transient429(`{"error":{"type":"rate_limit_error"}}`)}
		} else {
			chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("ok")}
		}
		close(chunks)
		return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
	}
	m.RegisterExecutor(exec)
	m.SetPluginAttemptFailurePolicy(attemptFailurePolicyFunc(func(context.Context, pluginapi.AttemptFailureRequest) (pluginapi.AttemptFailureResponse, bool, error) {
		return pluginapi.AttemptFailureResponse{RetrySameAuth: true}, true, nil
	}))
	result, err := m.ExecuteStream(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: "gpt-5"}, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	if result == nil {
		t.Fatal("ExecuteStream() result is nil")
	}
	chunk := <-result.Chunks
	if string(chunk.Payload) != "ok" || chunk.Err != nil {
		t.Fatalf("chunk = %#v, want payload", chunk)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestSameAuthRetry_StreamAfterPayloadDoesNotRetry(t *testing.T) {
	m := NewManager(nil, nil, nil)
	registerAttemptFailureAuth(t, m, "codex-stream-no-retry", "codex", "gpt-5")
	exec := &attemptFailureExecutor{identifier: "codex"}
	exec.stream = func(_ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
		chunks := make(chan cliproxyexecutor.StreamChunk, 2)
		chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("first")}
		chunks <- cliproxyexecutor.StreamChunk{Err: transient429(`{"error":{"type":"rate_limit_error"}}`)}
		close(chunks)
		return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
	}
	m.RegisterExecutor(exec)
	m.SetPluginAttemptFailurePolicy(attemptFailurePolicyFunc(func(context.Context, pluginapi.AttemptFailureRequest) (pluginapi.AttemptFailureResponse, bool, error) {
		t.Fatal("policy called after stream payload")
		return pluginapi.AttemptFailureResponse{}, true, nil
	}))
	result, err := m.ExecuteStream(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: "gpt-5"}, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	if result == nil {
		t.Fatal("ExecuteStream() result is nil")
	}
	if first := <-result.Chunks; string(first.Payload) != "first" {
		t.Fatalf("first chunk = %#v", first)
	}
	if second := <-result.Chunks; second.Err == nil {
		t.Fatalf("second chunk = %#v, want error", second)
	}
	if got := len(exec.callIDs()); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}
