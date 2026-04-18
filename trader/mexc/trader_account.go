package mexc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"nofx/logger"
	"strings"
	"time"
)

// GetBalance returns USDT futures balance.
func (t *MEXCTrader) GetBalance() (map[string]interface{}, error) {
	t.balanceCacheMutex.RLock()
	if t.cachedBalance != nil && time.Since(t.balanceCacheTime) < t.cacheDuration {
		cached := t.cachedBalance
		t.balanceCacheMutex.RUnlock()
		return cached, nil
	}
	t.balanceCacheMutex.RUnlock()

	data, err := t.doRequest(http.MethodGet, mexcAccountAssetsPath, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get MEXC balance: %w", err)
	}

	var assets []struct {
		Currency         string  `json:"currency"`
		AvailableBalance float64 `json:"availableBalance"`
		Equity           float64 `json:"equity"`
		Unrealized       float64 `json:"unrealized"`
		FrozenBalance    float64 `json:"frozenBalance"`
		PositionMargin   float64 `json:"positionMargin"`
	}
	if err := json.Unmarshal(data, &assets); err != nil {
		return nil, fmt.Errorf("failed to parse MEXC assets: %w, raw=%s", err, string(data))
	}

	var equity, available, unrealized float64
	for _, a := range assets {
		if strings.EqualFold(a.Currency, "USDT") {
			equity = a.Equity
			available = a.AvailableBalance
			unrealized = a.Unrealized
			break
		}
	}

	result := map[string]interface{}{
		"totalWalletBalance":    equity - unrealized,
		"availableBalance":      available,
		"totalUnrealizedProfit": unrealized,
		"total_equity":          equity,
	}

	t.balanceCacheMutex.Lock()
	t.cachedBalance = result
	t.balanceCacheTime = time.Now()
	t.balanceCacheMutex.Unlock()

	logger.Infof("✓ [MEXC] Balance: equity=%.2f, available=%.2f", equity, available)
	return result, nil
}

// GetMarketPrice returns last traded price for a symbol.
func (t *MEXCTrader) GetMarketPrice(symbol string) (float64, error) {
	sym := t.normalizeSymbol(symbol)

	params := url.Values{}
	params.Set("symbol", sym)

	data, err := t.doRequest(http.MethodGet, mexcContractTickerPath, params, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get MEXC price: %w", err)
	}

	// Ticker endpoint returns either a single object (when ?symbol=X) or an array.
	// Try object first.
	var single struct {
		Symbol    string  `json:"symbol"`
		LastPrice float64 `json:"lastPrice"`
	}
	if err := json.Unmarshal(data, &single); err == nil && single.Symbol != "" && single.LastPrice > 0 {
		return single.LastPrice, nil
	}

	var list []struct {
		Symbol    string  `json:"symbol"`
		LastPrice float64 `json:"lastPrice"`
	}
	if err := json.Unmarshal(data, &list); err != nil {
		return 0, fmt.Errorf("failed to parse ticker: %w", err)
	}
	for _, tk := range list {
		if tk.Symbol == sym {
			return tk.LastPrice, nil
		}
	}
	return 0, fmt.Errorf("no ticker data for %s", sym)
}

// SetLeverage sets leverage for a symbol (isolated margin, one-way mode).
// MEXC openType: 1 = isolated, 2 = cross.
// positionType: 1 = long (one-way), 2 = short. We default to 1 since NOFX uses one-way.
func (t *MEXCTrader) SetLeverage(symbol string, leverage int) error {
	sym := t.normalizeSymbol(symbol)

	body := map[string]interface{}{
		"symbol":       sym,
		"leverage":     leverage,
		"openType":     1,
		"positionType": 1,
	}

	_, err := t.doRequest(http.MethodPost, mexcPositionLeveragePath, nil, body)
	if err != nil {
		if strings.Contains(err.Error(), "same") || strings.Contains(err.Error(), "not changed") {
			return nil
		}
		logger.Infof("  ⚠️ [MEXC] Failed to set %s leverage: %v", sym, err)
		return err
	}

	logger.Infof("  ✓ [MEXC] %s leverage set to %dx (isolated)", sym, leverage)
	return nil
}

// SetMarginMode logs a warning if cross requested; MEXC supports per-symbol isolated/cross
// but NOFX behavior matches isolated. We return nil either way (non-fatal).
func (t *MEXCTrader) SetMarginMode(symbol string, isCrossMargin bool) error {
	if isCrossMargin {
		logger.Warnf("[MEXC] Cross margin not wired, using isolated for %s", t.normalizeSymbol(symbol))
	}
	return nil
}
