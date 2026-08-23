package helps

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestPooledUtlsCacheKeyIsolatesAuthAndProxy(t *testing.T) {
	authA := &cliproxyauth.Auth{ID: "auth-a"}
	authB := &cliproxyauth.Auth{ID: "auth-b"}

	keyA := pooledUtlsCacheKey(authA, "HTTPS://Proxy.Example:443")
	keyAEquivalent := pooledUtlsCacheKey(authA, "https://proxy.example:443")
	keyB := pooledUtlsCacheKey(authB, "https://proxy.example:443")
	keyOtherProxy := pooledUtlsCacheKey(authA, "https://other.example:443")

	if keyA != keyAEquivalent {
		t.Fatalf("equivalent proxy URLs produced different keys: %q != %q", keyA, keyAEquivalent)
	}
	if keyA == keyB {
		t.Fatal("different auth records shared a pooled transport key")
	}
	if keyA == keyOtherProxy {
		t.Fatal("different proxies shared a pooled transport key")
	}
	if strings.Contains(keyA, authA.ID) {
		t.Fatal("pooled transport key exposed the raw auth identifier")
	}
}

func TestCachedPooledUtlsRoundTripperReuseAndIsolation(t *testing.T) {
	authA := &cliproxyauth.Auth{ID: "pooled-cache-auth-a"}
	authB := &cliproxyauth.Auth{ID: "pooled-cache-auth-b"}
	first := cachedPooledUtlsRoundTripper(authA, "http://127.0.0.1:30101")
	second := cachedPooledUtlsRoundTripper(authA, "http://127.0.0.1:30101")
	if first != second {
		t.Fatal("same auth/proxy did not reuse pooled round tripper")
	}
	if first == cachedPooledUtlsRoundTripper(authB, "http://127.0.0.1:30101") {
		t.Fatal("different auth records reused pooled round tripper")
	}
	if first == cachedPooledUtlsRoundTripper(authA, "http://127.0.0.1:30102") {
		t.Fatal("different proxies reused pooled round tripper")
	}
}

func TestNewUtlsHTTPClientUsesPooledTransportOnlyWhenEnabled(t *testing.T) {
	auth := &cliproxyauth.Auth{ID: "transport-switch-auth"}

	dedicatedClient := NewUtlsHTTPClient(context.Background(), &config.Config{}, auth, 0)
	dedicatedFallback, ok := dedicatedClient.Transport.(*fallbackRoundTripper)
	if !ok {
		t.Fatalf("transport type = %T, want *fallbackRoundTripper", dedicatedClient.Transport)
	}
	if _, ok := dedicatedFallback.chrome.(*utlsRoundTripper); !ok {
		t.Fatalf("default chrome transport = %T, want dedicated uTLS", dedicatedFallback.chrome)
	}

	pooledClient := NewUtlsHTTPClient(context.Background(), &config.Config{Codex: config.CodexConfig{UpstreamTransport: "pooled"}}, auth, 0)
	pooledFallback, ok := pooledClient.Transport.(*fallbackRoundTripper)
	if !ok {
		t.Fatalf("pooled transport type = %T, want *fallbackRoundTripper", pooledClient.Transport)
	}
	if _, ok := pooledFallback.chrome.(*pooledUtlsRoundTripper); !ok {
		t.Fatalf("pooled chrome transport = %T, want *pooledUtlsRoundTripper", pooledFallback.chrome)
	}

	invalidClient := NewUtlsHTTPClient(context.Background(), &config.Config{Codex: config.CodexConfig{UpstreamTransport: "invalid"}}, auth, 0)
	invalidFallback := invalidClient.Transport.(*fallbackRoundTripper)
	if _, ok := invalidFallback.chrome.(*utlsRoundTripper); !ok {
		t.Fatalf("invalid mode chrome transport = %T, want dedicated fallback", invalidFallback.chrome)
	}
}

func TestPooledRetryRequestRequiresReplayableBody(t *testing.T) {
	noBodyReq, errNewRequest := http.NewRequest(http.MethodGet, "https://chatgpt.com/backend-api/codex/responses", nil)
	if errNewRequest != nil {
		t.Fatal(errNewRequest)
	}
	if retry, ok := pooledRetryRequest(noBodyReq); !ok || retry == noBodyReq {
		t.Fatal("expected a cloned no-body request for retry")
	}

	replayableReq, errNewRequest := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", bytes.NewReader([]byte(`{"input":"x"}`)))
	if errNewRequest != nil {
		t.Fatal(errNewRequest)
	}
	retry, ok := pooledRetryRequest(replayableReq)
	if !ok || retry == nil {
		t.Fatal("expected replayable request to be cloned")
	}
	if got, errRead := io.ReadAll(retry.Body); errRead != nil || string(got) != `{"input":"x"}` {
		t.Fatalf("retry body = %q, err = %v", got, errRead)
	}

	nonReplayableReq, errNewRequest := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", io.NopCloser(strings.NewReader("payload")))
	if errNewRequest != nil {
		t.Fatal(errNewRequest)
	}
	if _, ok := pooledRetryRequest(nonReplayableReq); ok {
		t.Fatal("non-replayable request unexpectedly allowed a retry")
	}
}
