package helps

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tls "github.com/refraction-networking/utls"
	internalcache "github.com/router-for-me/CLIProxyAPI/v7/internal/cache"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/http2"
	"golang.org/x/net/proxy"
	"golang.org/x/sync/singleflight"
)

const (
	pooledUtlsRoundTripperCacheCapacity = 128
	pooledUtlsReadIdleTimeout           = 30 * time.Second
	pooledUtlsPingTimeout               = 10 * time.Second
)

// pooledUtlsRoundTripper owns one or more persistent HTTP/2 connections for a
// single auth/proxy identity. Connections are keyed by upstream host and are
// never shared across auth records or proxies.
type pooledUtlsRoundTripper struct {
	dialer       proxy.Dialer
	sessionCache tls.ClientSessionCache

	mu       sync.Mutex
	closed   bool
	conns    map[string]*pooledUtlsConnection
	dialing  singleflight.Group
	poolHits atomic.Uint64
	poolMiss atomic.Uint64
}

type pooledUtlsConnection struct {
	addr string
	conn *http2.ClientConn
}

type pooledUtlsMetrics struct {
	poolHit         uint64
	poolMiss        uint64
	http2Reuse      bool
	responseHeaders time.Duration
	upstreamHost    string
	transportMode   string
}

func buildUtlsDialer(proxyURL string) proxy.Dialer {
	var dialer proxy.Dialer = proxy.Direct
	if proxyURL == "" {
		return dialer
	}
	proxyDialer, mode, errBuild := proxyutil.BuildDialer(proxyURL)
	if errBuild != nil {
		log.Errorf("utls pooled: failed to configure proxy dialer for %q: %v", proxyutil.Redact(proxyURL), errBuild)
	} else if mode != proxyutil.ModeInherit && proxyDialer != nil {
		dialer = proxyDialer
	}
	return dialer
}

func createUtlsH2Connection(ctx context.Context, dialer proxy.Dialer, sessionCache tls.ClientSessionCache, host, addr string, readIdleTimeout, pingTimeout time.Duration) (*http2.ClientConn, error) {
	contextDialer, ok := dialer.(proxy.ContextDialer)
	if !ok {
		return nil, errors.New("utls pooled: dialer does not support context cancellation")
	}
	conn, errDial := contextDialer.DialContext(ctx, "tcp", addr)
	if errDial != nil {
		return nil, fmt.Errorf("utls pooled: dial upstream: %w", errDial)
	}
	closeConn := func() {
		if errClose := conn.Close(); errClose != nil {
			log.Debugf("utls pooled: close failed upstream connection: %v", errClose)
		}
	}
	tlsConfig := &tls.Config{ServerName: host, ClientSessionCache: sessionCache}
	tlsConn := tls.UClient(conn, tlsConfig, tls.HelloChrome_Auto)
	if errHandshake := tlsConn.HandshakeContext(ctx); errHandshake != nil {
		closeConn()
		return nil, fmt.Errorf("utls pooled: TLS handshake: %w", errHandshake)
	}
	transport := &http2.Transport{
		ReadIdleTimeout: readIdleTimeout,
		PingTimeout:     pingTimeout,
	}
	h2Conn, errClientConn := transport.NewClientConn(tlsConn)
	if errClientConn != nil {
		if errClose := tlsConn.Close(); errClose != nil {
			log.Debugf("utls pooled: close TLS connection after HTTP/2 setup failure: %v", errClose)
		}
		return nil, fmt.Errorf("utls pooled: initialize HTTP/2 connection: %w", errClientConn)
	}
	return h2Conn, nil
}

func newPooledUtlsRoundTripper(proxyURL string) *pooledUtlsRoundTripper {
	return &pooledUtlsRoundTripper{
		dialer:       buildUtlsDialer(proxyURL),
		sessionCache: tls.NewLRUClientSessionCache(64),
		conns:        make(map[string]*pooledUtlsConnection),
	}
}

func (t *pooledUtlsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil {
		return nil, errors.New("utls pooled: request URL is nil")
	}
	hostname := req.URL.Hostname()
	if hostname == "" {
		return nil, errors.New("utls pooled: request host is empty")
	}
	port := req.URL.Port()
	if port == "" {
		port = "443"
	}
	addr := net.JoinHostPort(hostname, port)
	started := time.Now()
	connection, reused, errConn := t.connection(req.Context(), hostname, addr)
	if errConn != nil {
		return nil, errConn
	}

	resp, errRoundTrip := connection.conn.RoundTrip(req)
	if errRoundTrip != nil {
		t.invalidate(connection)
		if retryReq, ok := pooledRetryRequest(req); ok {
			retryConnection, retryReused, errRetryConn := t.connection(retryReq.Context(), hostname, addr)
			if errRetryConn == nil {
				resp, errRoundTrip = retryConnection.conn.RoundTrip(retryReq)
				if errRoundTrip == nil {
					connection = retryConnection
					reused = retryReused
					log.WithField("upstream_host", hostname).Debug("utls pooled rebuilt connection before response headers")
				} else {
					t.invalidate(retryConnection)
				}
			}
		}
		if errRoundTrip != nil {
			return nil, fmt.Errorf("utls pooled: HTTP/2 round trip: %w", errRoundTrip)
		}
	}
	if resp == nil {
		t.invalidate(connection)
		return nil, errors.New("utls pooled: upstream returned an empty response")
	}
	if resp.Body == nil {
		resp.Body = http.NoBody
	}

	metrics := pooledUtlsMetrics{
		poolHit:         t.poolHits.Load(),
		poolMiss:        t.poolMiss.Load(),
		http2Reuse:      reused,
		responseHeaders: time.Since(started),
		upstreamHost:    hostname,
		transportMode:   "pooled",
	}
	log.WithFields(log.Fields{
		"pool_event":          "response_headers",
		"pool_hit_total":      metrics.poolHit,
		"pool_miss_total":     metrics.poolMiss,
		"http2_reuse":         metrics.http2Reuse,
		"response_headers_ms": metrics.responseHeaders.Milliseconds(),
		"upstream_host":       metrics.upstreamHost,
		"transport_mode":      metrics.transportMode,
	}).Debug("utls pooled upstream response headers received")

	// Closing this body closes only the HTTP/2 stream. The connection remains in
	// the pool and can serve another request concurrently.
	return resp, nil
}

func pooledRetryRequest(req *http.Request) (*http.Request, bool) {
	if req == nil || req.Context().Err() != nil {
		return nil, false
	}
	if req.Body == nil || req.Body == http.NoBody {
		return req.Clone(req.Context()), true
	}
	if req.GetBody == nil {
		return nil, false
	}
	body, errGetBody := req.GetBody()
	if errGetBody != nil {
		return nil, false
	}
	retryReq := req.Clone(req.Context())
	retryReq.Body = body
	return retryReq, true
}

func (t *pooledUtlsRoundTripper) connection(ctx context.Context, host, addr string) (*pooledUtlsConnection, bool, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, false, errors.New("utls pooled: transport is closed")
	}
	if connection := t.conns[addr]; connection != nil {
		t.poolHits.Add(1)
		t.mu.Unlock()
		return connection, true, nil
	}
	t.mu.Unlock()

	value, err, _ := t.dialing.Do(addr, func() (any, error) {
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			return nil, errors.New("utls pooled: transport is closed")
		}
		if connection := t.conns[addr]; connection != nil {
			t.poolHits.Add(1)
			t.mu.Unlock()
			return connection, nil
		}
		t.mu.Unlock()

		t.poolMiss.Add(1)
		conn, errCreate := createUtlsH2Connection(ctx, t.dialer, t.sessionCache, host, addr, pooledUtlsReadIdleTimeout, pooledUtlsPingTimeout)
		if errCreate != nil {
			return nil, errCreate
		}
		connection := &pooledUtlsConnection{addr: addr, conn: conn}
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			_ = conn.Close()
			return nil, errors.New("utls pooled: transport closed during dial")
		}
		t.conns[addr] = connection
		t.mu.Unlock()
		return connection, nil
	})
	if err != nil {
		return nil, false, err
	}
	connection, ok := value.(*pooledUtlsConnection)
	if !ok || connection == nil || connection.conn == nil {
		return nil, false, errors.New("utls pooled: invalid pooled connection")
	}
	return connection, false, nil
}

func (t *pooledUtlsRoundTripper) invalidate(connection *pooledUtlsConnection) {
	if connection == nil {
		return
	}
	t.mu.Lock()
	if current := t.conns[connection.addr]; current == connection {
		delete(t.conns, connection.addr)
	}
	t.mu.Unlock()
	if errClose := connection.conn.Close(); errClose != nil {
		log.Debugf("utls pooled: close invalid connection: %v", errClose)
	}
}

func (t *pooledUtlsRoundTripper) CloseIdleConnections() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	connections := make([]*pooledUtlsConnection, 0, len(t.conns))
	for addr, connection := range t.conns {
		delete(t.conns, addr)
		connections = append(connections, connection)
	}
	t.mu.Unlock()
	for _, connection := range connections {
		if errClose := connection.conn.Close(); errClose != nil {
			log.Debugf("utls pooled: close idle connection: %v", errClose)
		}
	}
}

var pooledUtlsRoundTripperCache = internalcache.NewBoundedLRU[string, http.RoundTripper](
	pooledUtlsRoundTripperCacheCapacity,
	func(_ string, roundTripper http.RoundTripper) {
		if transport, ok := roundTripper.(interface{ CloseIdleConnections() }); ok {
			transport.CloseIdleConnections()
		}
	},
)

func cachedPooledUtlsRoundTripper(auth *cliproxyauth.Auth, proxyURL string) http.RoundTripper {
	key := pooledUtlsCacheKey(auth, proxyURL)
	return pooledUtlsRoundTripperCache.GetOrAdd(key, func() http.RoundTripper {
		return newPooledUtlsRoundTripper(proxyURL)
	})
}

func pooledUtlsCacheKey(auth *cliproxyauth.Auth, proxyURL string) string {
	identity := "anonymous"
	if auth != nil {
		identity = strings.TrimSpace(auth.ID)
		if identity == "" {
			identity = strings.TrimSpace(auth.Index)
		}
		if identity == "" {
			identity = strings.TrimSpace(auth.FileName)
		}
		if identity == "" {
			identity = "anonymous"
		}
	}
	hash := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(hash[:16]) + "\x00" + normalizeProxyForPool(proxyURL)
}

func normalizeProxyForPool(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "direct"
	}
	parsed, errParse := url.Parse(raw)
	if errParse != nil || parsed.Scheme == "" {
		return raw
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	parsed.Host = strings.ToLower(parsed.Host)
	return parsed.String()
}
