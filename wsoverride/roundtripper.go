package wsoverride

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// Freshness thresholds per endpoint; stale cache ⇒ fall through to real HTTP.
const (
	freshnessTicker    = 10 * time.Second
	freshnessMark      = 10 * time.Second
	freshnessAccount   = 15 * time.Second
	freshnessPositions = 15 * time.Second
	freshnessOrders    = 5 * time.Second
	ttlOpenInterest    = 60 * time.Second
	ttlExchangeInfo    = 24 * time.Hour
)

// Path fragments used for matching. We check URL.Path contains these — Binance
// base host is fixed (fapi.binance.com or fapi.binancefuture.com for testnet).
const (
	pathTickerPrice  = "/fapi/v1/ticker/price"
	pathPremiumIndex = "/fapi/v1/premiumIndex"
	pathOpenInterest = "/fapi/v1/openInterest"
	pathExchangeInfo = "/fapi/v1/exchangeInfo"
	pathAccountV2    = "/fapi/v2/account"
	pathPositionRisk = "/fapi/v2/positionRisk"
	pathOpenOrders   = "/fapi/v1/openOrders"
)

// Shared metrics counters (atomic).
type metrics struct {
	Hits   atomic.Int64
	Misses atomic.Int64
}

var globalMetrics metrics

// ---- Market-only transport (no userID context; used via SET_HTTP_CLIENT) ----

type marketOnlyTransport struct {
	marketCache *PriceCache
	restCache   *restResponseCache
	fallback    http.RoundTripper
	onMarketMiss func(symbol string)
}

func newMarketOnlyTransport(marketCache *PriceCache, fallback http.RoundTripper) *marketOnlyTransport {
	if fallback == nil {
		fallback = http.DefaultTransport
	}
	return &marketOnlyTransport{
		marketCache: marketCache,
		restCache:   newRESTResponseCache(),
		fallback:    fallback,
	}
}

func (t *marketOnlyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet {
		return t.fallback.RoundTrip(req)
	}
	p := req.URL.Path
	switch {
	case strings.Contains(p, pathTickerPrice):
		if resp, ok := handleTicker(req, t.marketCache); ok {
			if t.onMarketMiss == nil && resp == nil {
				t.onMarketMiss = func(string) {}
			}
			globalMetrics.Hits.Add(1)
			return resp, nil
		}
		ensureMarketSub(req, t.onMarketMiss)
	case strings.Contains(p, pathPremiumIndex):
		if resp, ok := handleMark(req, t.marketCache); ok {
			globalMetrics.Hits.Add(1)
			return resp, nil
		}
		ensureMarketSub(req, t.onMarketMiss)
	case strings.Contains(p, pathOpenInterest):
		if body, ok := t.restCache.get(req.URL.String()); ok {
			globalMetrics.Hits.Add(1)
			return synthResponse(req, body), nil
		}
		resp, err := t.fallback.RoundTrip(req)
		return storeRESTResponse(resp, err, req, t.restCache, ttlOpenInterest)
	case strings.Contains(p, pathExchangeInfo):
		if body, ok := t.restCache.get(req.URL.String()); ok {
			globalMetrics.Hits.Add(1)
			return synthResponse(req, body), nil
		}
		resp, err := t.fallback.RoundTrip(req)
		return storeRESTResponse(resp, err, req, t.restCache, ttlExchangeInfo)
	}
	globalMetrics.Misses.Add(1)
	return t.fallback.RoundTrip(req)
}

// ---- Per-user transport (used via NEW_BINANCE_TRADER) ----

type perUserTransport struct {
	userID      string
	userCache   *UserStateCache
	marketCache *PriceCache
	restCache   *restResponseCache
	fallback    http.RoundTripper
	onMarketMiss func(symbol string)
}

func newPerUserTransport(userID string, userCache *UserStateCache, marketCache *PriceCache, fallback http.RoundTripper) *perUserTransport {
	if fallback == nil {
		fallback = http.DefaultTransport
	}
	return &perUserTransport{
		userID:      userID,
		userCache:   userCache,
		marketCache: marketCache,
		restCache:   newRESTResponseCache(),
		fallback:    fallback,
	}
}

func (t *perUserTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet {
		return t.fallback.RoundTrip(req)
	}
	p := req.URL.Path

	// Market endpoints (shared across users)
	switch {
	case strings.Contains(p, pathTickerPrice):
		if resp, ok := handleTicker(req, t.marketCache); ok {
			globalMetrics.Hits.Add(1)
			return resp, nil
		}
		ensureMarketSub(req, t.onMarketMiss)
	case strings.Contains(p, pathPremiumIndex):
		if resp, ok := handleMark(req, t.marketCache); ok {
			globalMetrics.Hits.Add(1)
			return resp, nil
		}
		ensureMarketSub(req, t.onMarketMiss)
	case strings.Contains(p, pathOpenInterest):
		if body, ok := t.restCache.get(req.URL.String()); ok {
			globalMetrics.Hits.Add(1)
			return synthResponse(req, body), nil
		}
		resp, err := t.fallback.RoundTrip(req)
		return storeRESTResponse(resp, err, req, t.restCache, ttlOpenInterest)
	case strings.Contains(p, pathExchangeInfo):
		if body, ok := t.restCache.get(req.URL.String()); ok {
			globalMetrics.Hits.Add(1)
			return synthResponse(req, body), nil
		}
		resp, err := t.fallback.RoundTrip(req)
		return storeRESTResponse(resp, err, req, t.restCache, ttlExchangeInfo)
	case strings.Contains(p, pathAccountV2):
		if resp, ok := handleAccount(req, t.userID, t.userCache); ok {
			globalMetrics.Hits.Add(1)
			return resp, nil
		}
	case strings.Contains(p, pathPositionRisk):
		if resp, ok := handlePositions(req, t.userID, t.userCache); ok {
			globalMetrics.Hits.Add(1)
			return resp, nil
		}
	case strings.Contains(p, pathOpenOrders):
		if resp, ok := handleOpenOrders(req, t.userID, t.userCache); ok {
			globalMetrics.Hits.Add(1)
			return resp, nil
		}
	}
	globalMetrics.Misses.Add(1)
	return t.fallback.RoundTrip(req)
}

// ---- Handlers ----

func handleTicker(req *http.Request, cache *PriceCache) (*http.Response, bool) {
	sym := strings.ToUpper(req.URL.Query().Get("symbol"))
	if sym == "" || cache == nil {
		return nil, false
	}
	snap, ok := cache.GetTicker(sym)
	if !ok || time.Since(snap.UpdatedAt) > freshnessTicker {
		return nil, false
	}
	body := fmt.Sprintf(`{"symbol":"%s","price":"%s","time":%d}`,
		snap.Symbol, formatFloat(snap.LastPrice), snap.UpdatedAt.UnixMilli())
	return synthResponse(req, []byte(body)), true
}

func handleMark(req *http.Request, cache *PriceCache) (*http.Response, bool) {
	sym := strings.ToUpper(req.URL.Query().Get("symbol"))
	if sym == "" || cache == nil {
		return nil, false
	}
	snap, ok := cache.GetMarkPrice(sym)
	if !ok || time.Since(snap.UpdatedAt) > freshnessMark {
		return nil, false
	}
	body := fmt.Sprintf(
		`{"symbol":"%s","markPrice":"%s","indexPrice":"%s","estimatedSettlePrice":"%s","lastFundingRate":"%s","interestRate":"%s","nextFundingTime":%d,"time":%d}`,
		snap.Symbol,
		formatFloat(snap.MarkPrice),
		formatFloat(snap.IndexPrice),
		formatFloat(snap.EstSettlePrice),
		formatFloat(snap.LastFundingRate),
		formatFloat(snap.InterestRate),
		snap.NextFundingTime,
		snap.UpdatedAt.UnixMilli(),
	)
	return synthResponse(req, []byte(body)), true
}

func handleAccount(req *http.Request, userID string, cache *UserStateCache) (*http.Response, bool) {
	snap, ok := cache.GetAccount(userID)
	if !ok || snap == nil {
		return nil, false
	}
	// If WS stream healthy (connected + primed), serve regardless of age —
	// deltas arrive via WS so absence of events means state is unchanged.
	if !UserStreamHealthy(userID) && time.Since(snap.UpdatedAt) > freshnessAccount {
		return nil, false
	}
	body := snap.RawJSON
	// If WS has patched balances, re-render via template-merge of raw.
	if snap.FromSource == "ws-push" && len(snap.RawJSON) > 0 {
		if patched, err := patchBalances(snap.RawJSON, snap.Balances); err == nil {
			body = patched
		}
	}
	if len(body) == 0 {
		return nil, false
	}
	return synthResponse(req, body), true
}

func handlePositions(req *http.Request, userID string, cache *UserStateCache) (*http.Response, bool) {
	snap, ok := cache.GetPositions(userID)
	if !ok || snap == nil {
		return nil, false
	}
	if !UserStreamHealthy(userID) && time.Since(snap.UpdatedAt) > freshnessPositions {
		return nil, false
	}
	body := snap.RawJSON
	if snap.FromSource == "ws-push" && len(snap.RawJSON) > 0 {
		if patched, err := patchPositions(snap.RawJSON, snap.Positions); err == nil {
			body = patched
		}
	}
	if len(body) == 0 {
		return nil, false
	}
	// Filter by symbol query param if present.
	if sym := req.URL.Query().Get("symbol"); sym != "" {
		if filtered, err := filterPositionsBySymbol(body, strings.ToUpper(sym)); err == nil {
			body = filtered
		}
	}
	return synthResponse(req, body), true
}

func handleOpenOrders(req *http.Request, userID string, cache *UserStateCache) (*http.Response, bool) {
	snap, ok := cache.GetOpenOrders(userID)
	if !ok || snap == nil {
		return nil, false
	}
	if !UserStreamHealthy(userID) && time.Since(snap.UpdatedAt) > freshnessOrders {
		return nil, false
	}
	// For ws-push snapshots we don't have rawJSON — render from parsed orders.
	body, err := renderOrders(snap, strings.ToUpper(req.URL.Query().Get("symbol")))
	if err != nil {
		return nil, false
	}
	return synthResponse(req, body), true
}

// ---- helpers ----

func synthResponse(req *http.Request, body []byte) *http.Response {
	return &http.Response{
		Status:        "200 OK",
		StatusCode:    200,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{"Content-Type": {"application/json"}},
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}

func storeRESTResponse(resp *http.Response, err error, req *http.Request, cache *restResponseCache, ttl time.Duration) (*http.Response, error) {
	globalMetrics.Misses.Add(1)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		return resp, err
	}
	body, rerr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if rerr != nil {
		return resp, rerr
	}
	cache.set(req.URL.String(), body, ttl)
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	return resp, nil
}

func ensureMarketSub(req *http.Request, fn func(symbol string)) {
	if fn == nil {
		return
	}
	if sym := req.URL.Query().Get("symbol"); sym != "" {
		fn(sym)
	}
}

func formatFloat(f float64) string {
	// Binance represents values as strings with up to 8 dp.
	return strings.TrimRight(strings.TrimRight(fmtFloat(f), "0"), ".")
}

func fmtFloat(f float64) string {
	// Use %.8f to preserve precision, then trim trailing zeros above.
	return fmt.Sprintf("%.8f", f)
}

// patchBalances replaces fields in raw account JSON with current balance map.
// Returns the patched JSON. If raw is missing the expected keys, returns as-is.
func patchBalances(raw []byte, balances map[string]AssetBalance) ([]byte, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	if assetsAny, ok := m["assets"].([]any); ok {
		for _, a := range assetsAny {
			am, ok := a.(map[string]any)
			if !ok {
				continue
			}
			name, _ := am["asset"].(string)
			if b, ok := balances[name]; ok {
				am["walletBalance"] = formatFloat(b.WalletBalance)
				am["crossWalletBalance"] = formatFloat(b.CrossWalletBalance)
			}
		}
	}
	return json.Marshal(m)
}

func patchPositions(raw []byte, positions []Position) ([]byte, error) {
	// positionRisk response is a JSON array of objects keyed by symbol+positionSide.
	var arr []map[string]any
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, err
	}
	index := map[string]Position{}
	for _, p := range positions {
		index[p.Symbol+"|"+p.PositionSide] = p
	}
	for _, item := range arr {
		sym, _ := item["symbol"].(string)
		ps, _ := item["positionSide"].(string)
		if p, ok := index[sym+"|"+ps]; ok {
			item["positionAmt"] = formatFloat(p.PositionAmt)
			item["entryPrice"] = formatFloat(p.EntryPrice)
			item["unRealizedProfit"] = formatFloat(p.UnrealizedProfit)
		}
	}
	return json.Marshal(arr)
}

func filterPositionsBySymbol(raw []byte, sym string) ([]byte, error) {
	var arr []map[string]any
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, err
	}
	out := arr[:0]
	for _, item := range arr {
		if s, _ := item["symbol"].(string); strings.EqualFold(s, sym) {
			out = append(out, item)
		}
	}
	return json.Marshal(out)
}

// renderOrders builds a minimal Binance-shaped JSON array for open orders from
// parsed snapshot. Fields cover what SDK struct futures.Order exposes.
func renderOrders(snap *OrdersSnapshot, filterSym string) ([]byte, error) {
	out := make([]map[string]any, 0, len(snap.Orders))
	for _, o := range snap.Orders {
		if filterSym != "" && !strings.EqualFold(o.Symbol, filterSym) {
			continue
		}
		out = append(out, map[string]any{
			"symbol":        o.Symbol,
			"orderId":       o.OrderID,
			"clientOrderId": o.ClientOrderID,
			"side":          o.Side,
			"positionSide":  o.PositionSide,
			"type":          o.Type,
			"status":        o.Status,
			"price":         formatFloat(o.Price),
			"origQty":       formatFloat(o.OrigQty),
			"executedQty":   formatFloat(o.ExecutedQty),
			"updateTime":    o.UpdateTime,
			"time":          o.UpdateTime,
			"reduceOnly":    false,
			"closePosition": false,
			"timeInForce":   "GTC",
			"workingType":   "CONTRACT_PRICE",
		})
	}
	return json.Marshal(out)
}
