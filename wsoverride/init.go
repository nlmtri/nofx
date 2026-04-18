package wsoverride

import (
	"context"
	"log"
	"net/http"
	"sync"

	"github.com/adshao/go-binance/v2/futures"

	"nofx/hook"
)

// Package-level state. Initialised lazily on first hook invocation so the
// process does not open WS connections merely by importing this package.
var (
	initOnce     sync.Once
	rootCtx      context.Context
	rootCancel   context.CancelFunc
	marketCache  *PriceCache
	userCache    *UserStateCache
	marketStream *MarketStreamClient

	userStreamsMu sync.Mutex
	userStreams   = map[string]*UserDataStream{}
)

// UserStreamHealthy reports whether a user stream exists and is currently
// connected + primed. Used by the RoundTripper to bypass wall-clock freshness
// gating when live deltas are flowing.
func UserStreamHealthy(userID string) bool {
	userStreamsMu.Lock()
	s, ok := userStreams[userID]
	userStreamsMu.Unlock()
	if !ok || s == nil {
		return false
	}
	return s.Healthy()
}

func init() {
	hook.RegisterHook(hook.SET_HTTP_CLIENT, onSetHTTPClient)
	hook.RegisterHook(hook.NEW_BINANCE_TRADER, onNewBinanceTrader)
	log.Printf("🪝 wsoverride: hooks SET_HTTP_CLIENT + NEW_BINANCE_TRADER registered")
}

func ensureInit() {
	initOnce.Do(func() {
		rootCtx, rootCancel = context.WithCancel(context.Background())
		marketCache = NewPriceCache()
		userCache = NewUserStateCache()
		marketStream = NewMarketStreamClient(marketCache)
		go func() {
			if err := marketStream.Start(rootCtx); err != nil && rootCtx.Err() == nil {
				log.Printf("⚠️ market stream exited: %v", err)
			}
		}()
		go metricsLogger(rootCtx)
	})
}

func metricsLogger(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	var prevHits, prevMisses int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h := globalMetrics.Hits.Load()
			m := globalMetrics.Misses.Load()
			dh := h - prevHits
			dm := m - prevMisses
			prevHits, prevMisses = h, m
			total := h + m
			hitPct := 0.0
			if total > 0 {
				hitPct = float64(h) / float64(total) * 100
			}
			log.Printf("📊 wsoverride 30s: hits=%d (+%d) misses=%d (+%d) hit_rate=%.1f%%",
				h, dh, m, dm, hitPct)
		}
	}
}

// Shutdown stops the background WS connections. Call from main if desired.
func Shutdown() {
	if rootCancel != nil {
		rootCancel()
	}
}

// onSetHTTPClient handles the SET_HTTP_CLIENT hook — wraps a plain *http.Client
// used by the public market API client with the market-only transport.
func onSetHTTPClient(args ...any) any {
	if len(args) < 1 {
		return &hook.SetHttpClientResult{Err: errInsufficientArgs("SET_HTTP_CLIENT", 1, len(args))}
	}
	client, ok := args[0].(*http.Client)
	if !ok || client == nil {
		return &hook.SetHttpClientResult{Err: errBadArg("SET_HTTP_CLIENT", 0, "*http.Client")}
	}
	ensureInit()
	mt := newMarketOnlyTransport(marketCache, client.Transport)
	mt.onMarketMiss = func(symbol string) { marketStream.EnsureSubscribed(symbol) }
	wrapped := &http.Client{
		Transport:     mt,
		CheckRedirect: client.CheckRedirect,
		Jar:           client.Jar,
		Timeout:       client.Timeout,
	}
	return &hook.SetHttpClientResult{Client: wrapped}
}

// onNewBinanceTrader handles NEW_BINANCE_TRADER — replaces the client's
// transport with a per-user cache-aware transport and spawns (once) a user data
// stream for that userID.
func onNewBinanceTrader(args ...any) any {
	if len(args) < 2 {
		return &hook.NewBinanceTraderResult{Err: errInsufficientArgs("NEW_BINANCE_TRADER", 2, len(args))}
	}
	userID, _ := args[0].(string)
	client, ok := args[1].(*futures.Client)
	if !ok || client == nil {
		return &hook.NewBinanceTraderResult{Err: errBadArg("NEW_BINANCE_TRADER", 1, "*futures.Client")}
	}
	ensureInit()
	if client.HTTPClient == nil {
		client.HTTPClient = &http.Client{}
	}
	fallback := client.HTTPClient.Transport
	pt := newPerUserTransport(userID, userCache, marketCache, fallback)
	pt.onMarketMiss = func(symbol string) { marketStream.EnsureSubscribed(symbol) }
	client.HTTPClient.Transport = pt

	// Spawn user stream once per userID using a SEPARATE client that does NOT
	// wrap its transport — prevents the prime REST calls from recursing into
	// our RoundTripper.
	userStreamsMu.Lock()
	defer userStreamsMu.Unlock()
	if _, exists := userStreams[userID]; !exists {
		bareClient := cloneFuturesClientBare(client)
		stream := NewUserDataStream(userID, bareClient, userCache)
		userStreams[userID] = stream
		go func() {
			if err := stream.Start(rootCtx); err != nil && rootCtx.Err() == nil {
				log.Printf("⚠️ user stream[%s] exited: %v", safeShort(userID), err)
			}
		}()
	}
	return &hook.NewBinanceTraderResult{Client: client}
}

// cloneFuturesClientBare constructs a sibling futures.Client that shares the
// same API credentials but uses a bare *http.Client with no overridden
// Transport — used for REST prime and listenKey management so those calls are
// NOT re-intercepted by our own RoundTripper (which would cause a loop / stale
// serve during prime).
func cloneFuturesClientBare(c *futures.Client) *futures.Client {
	bare := futures.NewClient(c.APIKey, c.SecretKey)
	if c.BaseURL != "" {
		bare.BaseURL = c.BaseURL
	}
	bare.HTTPClient = &http.Client{Timeout: c.HTTPClient.Timeout}
	return bare
}

func safeShort(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}
