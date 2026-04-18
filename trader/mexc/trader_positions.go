package mexc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"nofx/trader/types"
	"strconv"
	"time"
)

// MEXC position raw shape (private/position/open_positions).
type mexcPositionRaw struct {
	Symbol         string  `json:"symbol"`
	PositionID     int64   `json:"positionId"`
	PositionType   int     `json:"positionType"`   // 1=long, 2=short
	State          int     `json:"state"`          // 1=holding, 2=closed, 3=liquidated
	HoldVol        float64 `json:"holdVol"`        // contracts held
	OpenAvgPrice   float64 `json:"openAvgPrice"`
	HoldAvgPrice   float64 `json:"holdAvgPrice"`
	Leverage       int     `json:"leverage"`
	LiquidatePrice float64 `json:"liquidatePrice"`
	RealizedPnl    float64 `json:"realized"`
	CreateTime     int64   `json:"createTime"`
	UpdateTime     int64   `json:"updateTime"`
}

// GetPositions returns all open positions.
func (t *MEXCTrader) GetPositions() ([]map[string]interface{}, error) {
	t.positionsCacheMutex.RLock()
	if t.cachedPositions != nil && time.Since(t.positionsCacheTime) < t.cacheDuration {
		cached := t.cachedPositions
		t.positionsCacheMutex.RUnlock()
		return cached, nil
	}
	t.positionsCacheMutex.RUnlock()

	data, err := t.doRequest(http.MethodGet, mexcPositionOpenPath, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get MEXC positions: %w", err)
	}

	var raw []mexcPositionRaw
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse positions: %w", err)
	}

	result := make([]map[string]interface{}, 0, len(raw))
	for _, p := range raw {
		if p.State != 1 || p.HoldVol == 0 {
			continue
		}

		qty, err := t.contractsToQty(p.Symbol, int64(p.HoldVol))
		if err != nil {
			qty = p.HoldVol // fallback: treat vol as base qty
		}

		side := "long"
		if p.PositionType == 2 {
			side = "short"
		}

		// Fetch mark price on-demand (ticker call; cheap and matches bitget pattern).
		mark, _ := t.GetMarketPrice(p.Symbol)

		entry := p.OpenAvgPrice
		if entry == 0 {
			entry = p.HoldAvgPrice
		}

		var unrealized float64
		if mark > 0 && entry > 0 {
			if side == "long" {
				unrealized = (mark - entry) * qty
			} else {
				unrealized = (entry - mark) * qty
			}
		}

		result = append(result, map[string]interface{}{
			"symbol":           p.Symbol,
			"positionAmt":      qty,
			"entryPrice":       entry,
			"markPrice":        mark,
			"unRealizedProfit": unrealized,
			"leverage":         float64(p.Leverage),
			"liquidationPrice": p.LiquidatePrice,
			"side":             side,
			"createdTime":      p.CreateTime,
			"updatedTime":      p.UpdateTime,
		})
	}

	t.positionsCacheMutex.Lock()
	t.cachedPositions = result
	t.positionsCacheTime = time.Now()
	t.positionsCacheMutex.Unlock()

	return result, nil
}

// GetClosedPnL retrieves closed position PnL records from MEXC order history.
// MEXC does not expose a position-history endpoint, so we aggregate closing deals.
func (t *MEXCTrader) GetClosedPnL(startTime time.Time, limit int) ([]types.ClosedPnLRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 100 {
		limit = 100
	}

	params := url.Values{}
	params.Set("start_time", strconv.FormatInt(startTime.UnixMilli(), 10))
	params.Set("end_time", strconv.FormatInt(time.Now().UnixMilli(), 10))
	params.Set("page_num", "1")
	params.Set("page_size", strconv.Itoa(limit))

	data, err := t.doRequest(http.MethodGet, mexcOrderDealsPath, params, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get MEXC deals: %w", err)
	}

	var deals []struct {
		Symbol      string  `json:"symbol"`
		OrderID     string  `json:"orderId"`
		Side        int     `json:"side"`        // 1=open long, 2=close short, 3=open short, 4=close long
		Price       float64 `json:"price"`
		Vol         float64 `json:"vol"`         // contracts
		Leverage    int     `json:"leverage"`
		Fee         float64 `json:"fee"`
		FeeCurrency string  `json:"feeCurrency"`
		Profit      float64 `json:"profit"`
		Timestamp   int64   `json:"timestamp"`
	}
	if err := json.Unmarshal(data, &deals); err != nil {
		return nil, fmt.Errorf("failed to parse MEXC deals: %w", err)
	}

	records := make([]types.ClosedPnLRecord, 0, len(deals))
	for _, d := range deals {
		// Only closing deals have realized pnl.
		if d.Side != 2 && d.Side != 4 {
			continue
		}

		qty, err := t.contractsToQty(d.Symbol, int64(d.Vol))
		if err != nil {
			qty = d.Vol
		}

		side := "long"
		if d.Side == 2 {
			// Close short = we had short position.
			side = "short"
		}

		rec := types.ClosedPnLRecord{
			Symbol:      d.Symbol,
			Side:        side,
			ExitPrice:   d.Price,
			Quantity:    qty,
			RealizedPnL: d.Profit,
			Fee:         -d.Fee,
			Leverage:    d.Leverage,
			ExitTime:    time.UnixMilli(d.Timestamp).UTC(),
			EntryTime:   time.UnixMilli(d.Timestamp).UTC(),
			OrderID:     d.OrderID,
			CloseType:   "unknown",
		}
		records = append(records, rec)
	}

	return records, nil
}
