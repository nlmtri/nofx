package mexc

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"nofx/logger"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MEXC Contract API endpoints
const (
	mexcBaseURL = "https://contract.mexc.com"

	// Public
	mexcContractDetailPath = "/api/v1/contract/detail"
	mexcContractTickerPath = "/api/v1/contract/ticker"
	mexcContractKlinePath  = "/api/v1/contract/kline"
	mexcContractPingPath   = "/api/v1/contract/ping"

	// Private - account
	mexcAccountAssetsPath = "/api/v1/private/account/assets"

	// Private - positions
	mexcPositionOpenPath       = "/api/v1/private/position/open_positions"
	mexcPositionLeveragePath   = "/api/v1/private/position/change_leverage"
	mexcPositionMarginTypePath = "/api/v1/private/position/change_margin"

	// Private - orders
	mexcOrderSubmitPath      = "/api/v1/private/order/create"
	mexcOrderCancelPath      = "/api/v1/private/order/cancel"
	mexcOrderCancelAllPath   = "/api/v1/private/order/cancel_all"
	mexcOrderListOpenPath    = "/api/v1/private/order/list/open_orders"
	mexcOrderListHistoryPath = "/api/v1/private/order/list/history_orders"
	mexcOrderGetByIDPath     = "/api/v1/private/order/get"

	// Private - plan orders (SL/TP)
	mexcPlanOrderPlacePath  = "/api/v1/private/planorder/place/v2"
	mexcPlanOrderCancelPath = "/api/v1/private/planorder/cancel"
	mexcPlanOrderListPath   = "/api/v1/private/planorder/list/orders"
)

// MEXCTrader MEXC futures trader
type MEXCTrader struct {
	apiKey    string
	secretKey string

	httpClient *http.Client

	// Balance cache
	cachedBalance     map[string]interface{}
	balanceCacheTime  time.Time
	balanceCacheMutex sync.RWMutex

	// Positions cache
	cachedPositions     []map[string]interface{}
	positionsCacheTime  time.Time
	positionsCacheMutex sync.RWMutex

	// Contract info cache
	contractsCache      map[string]*MEXCContract
	contractsCacheTime  time.Time
	contractsCacheMutex sync.RWMutex

	cacheDuration time.Duration
}

// MEXCContract contract spec from /api/v1/contract/detail
type MEXCContract struct {
	Symbol       string  // e.g. "BTC_USDT"
	BaseCoin     string  // e.g. "BTC"
	QuoteCoin    string  // e.g. "USDT"
	ContractSize float64 // 1 contract = ContractSize base coin (critical for qty conversion)
	PriceScale   int     // price decimals
	VolScale     int     // volume decimals (for user-facing qty formatting)
	MinVol       float64 // min contract count per order
	MaxVol       float64 // max contract count per order
}

// MEXCResponse standard MEXC envelope
type MEXCResponse struct {
	Success bool            `json:"success"`
	Code    int             `json:"code"`
	Data    json.RawMessage `json:"data"`
	Message string          `json:"message"`
}

// NewMEXCTrader creates a MEXC trader
func NewMEXCTrader(apiKey, secretKey string) *MEXCTrader {
	httpClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: http.DefaultTransport,
	}

	trader := &MEXCTrader{
		apiKey:         apiKey,
		secretKey:      secretKey,
		httpClient:     httpClient,
		cacheDuration:  15 * time.Second,
		contractsCache: make(map[string]*MEXCContract),
	}

	logger.Infof("🟢 [MEXC] Trader initialized")
	return trader
}

// sign generates MEXC signature headers.
//   - GET : payload = apiKey + timestamp + sortedQueryString
//   - POST: payload = apiKey + timestamp + jsonBody
// Signature = hex(HMAC_SHA256(payload, secretKey))
func (t *MEXCTrader) sign(timestamp, payload string) string {
	pre := t.apiKey + timestamp + payload
	h := hmac.New(sha256.New, []byte(t.secretKey))
	h.Write([]byte(pre))
	return hex.EncodeToString(h.Sum(nil))
}

// buildSortedQuery sorts params alphabetically and returns the query string.
func buildSortedQuery(params url.Values) string {
	if len(params) == 0 {
		return ""
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		for _, v := range params[k] {
			parts = append(parts, k+"="+v)
		}
	}
	return strings.Join(parts, "&")
}

// doRequest performs a MEXC HTTP request.
//   - For GET: params are encoded & signed; body should be nil.
//   - For POST: body is JSON-marshalled & signed.
func (t *MEXCTrader) doRequest(method, path string, params url.Values, body interface{}) (json.RawMessage, error) {
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)

	var bodyBytes []byte
	var err error
	var signPayload string
	fullURL := mexcBaseURL + path
	isAuth := strings.Contains(path, "/private/")

	if method == http.MethodGet {
		qs := buildSortedQuery(params)
		if qs != "" {
			fullURL += "?" + qs
		}
		signPayload = qs
	} else {
		if body != nil {
			bodyBytes, err = json.Marshal(body)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize request body: %w", err)
			}
			signPayload = string(bodyBytes)
		}
	}

	req, err := http.NewRequest(method, fullURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if isAuth {
		// Bypass Go's header canonicalisation — MEXC validates exact case `ApiKey`.
		req.Header["ApiKey"] = []string{t.apiKey}
		req.Header["Request-Time"] = []string{timestamp}
		req.Header["Signature"] = []string{t.sign(timestamp, signPayload)}
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	// MEXC CDN (Akamai) rejects default Go User-Agent. Set a browser-like UA.
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var envelope MEXCResponse
	if err := json.Unmarshal(respBody, &envelope); err != nil {
		return nil, fmt.Errorf("failed to parse MEXC response: %w, body=%s", err, string(respBody))
	}

	if !envelope.Success || (envelope.Code != 0 && envelope.Code != 200) {
		return nil, fmt.Errorf("MEXC API error: code=%d, message=%s", envelope.Code, envelope.Message)
	}

	return envelope.Data, nil
}

// normalizeSymbol converts "BTCUSDT" → "BTC_USDT". Idempotent.
func (t *MEXCTrader) normalizeSymbol(symbol string) string {
	s := strings.ToUpper(strings.TrimSpace(symbol))
	if strings.Contains(s, "_") {
		return s
	}
	// Common quote suffixes — USDT first (most common for MEXC perpetuals).
	for _, quote := range []string{"USDT", "USDC", "USD"} {
		if strings.HasSuffix(s, quote) && len(s) > len(quote) {
			return s[:len(s)-len(quote)] + "_" + quote
		}
	}
	return s
}

// getContract returns contract spec, caching the full list 5min.
func (t *MEXCTrader) getContract(symbol string) (*MEXCContract, error) {
	symbol = t.normalizeSymbol(symbol)

	t.contractsCacheMutex.RLock()
	if c, ok := t.contractsCache[symbol]; ok && time.Since(t.contractsCacheTime) < 5*time.Minute {
		t.contractsCacheMutex.RUnlock()
		return c, nil
	}
	t.contractsCacheMutex.RUnlock()

	data, err := t.doRequest(http.MethodGet, mexcContractDetailPath, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get contract detail: %w", err)
	}

	var raw []struct {
		Symbol       string  `json:"symbol"`
		BaseCoin     string  `json:"baseCoin"`
		QuoteCoin    string  `json:"quoteCoin"`
		ContractSize float64 `json:"contractSize"`
		PriceScale   int     `json:"priceScale"`
		VolScale     int     `json:"volScale"`
		MinVol       float64 `json:"minVol"`
		MaxVol       float64 `json:"maxVol"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse contract detail: %w", err)
	}

	t.contractsCacheMutex.Lock()
	defer t.contractsCacheMutex.Unlock()
	for _, c := range raw {
		cs := c.ContractSize
		if cs == 0 {
			cs = 1
		}
		t.contractsCache[c.Symbol] = &MEXCContract{
			Symbol:       c.Symbol,
			BaseCoin:     c.BaseCoin,
			QuoteCoin:    c.QuoteCoin,
			ContractSize: cs,
			PriceScale:   c.PriceScale,
			VolScale:     c.VolScale,
			MinVol:       c.MinVol,
			MaxVol:       c.MaxVol,
		}
	}
	t.contractsCacheTime = time.Now()

	if c, ok := t.contractsCache[symbol]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("contract not found: %s", symbol)
}

// qtyToContracts converts user base-coin quantity → MEXC contract count (int64).
func (t *MEXCTrader) qtyToContracts(symbol string, qty float64) (int64, error) {
	c, err := t.getContract(symbol)
	if err != nil {
		return 0, err
	}
	if c.ContractSize <= 0 {
		return 0, fmt.Errorf("invalid contractSize=%f for %s", c.ContractSize, symbol)
	}
	contracts := qty / c.ContractSize
	// Round to nearest integer (MEXC uses integer contract counts).
	return int64(math.Round(contracts)), nil
}

// contractsToQty converts MEXC contract count → user base-coin quantity.
func (t *MEXCTrader) contractsToQty(symbol string, contracts int64) (float64, error) {
	c, err := t.getContract(symbol)
	if err != nil {
		return 0, err
	}
	return float64(contracts) * c.ContractSize, nil
}

// FormatQuantity formats quantity to volScale decimals.
func (t *MEXCTrader) FormatQuantity(symbol string, quantity float64) (string, error) {
	c, err := t.getContract(symbol)
	if err != nil {
		return strconv.FormatFloat(quantity, 'f', 4, 64), nil
	}
	return strconv.FormatFloat(quantity, 'f', c.VolScale, 64), nil
}

// clearCache invalidates balance + positions caches after mutations.
func (t *MEXCTrader) clearCache() {
	t.balanceCacheMutex.Lock()
	t.cachedBalance = nil
	t.balanceCacheMutex.Unlock()

	t.positionsCacheMutex.Lock()
	t.cachedPositions = nil
	t.positionsCacheMutex.Unlock()
}

// genMEXCExternalOID generates unique external order ID (<=32 chars for MEXC).
func genMEXCExternalOID() string {
	ts := time.Now().UnixNano() % 10000000000000
	rnd := time.Now().Nanosecond() % 100000
	return fmt.Sprintf("nofx%d%05d", ts, rnd)
}
