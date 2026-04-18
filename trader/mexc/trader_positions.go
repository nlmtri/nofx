package mexc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"nofx/trader/types"
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

// GetClosedPnL retrieves closed position PnL records from MEXC history orders.
// Filters state=3 (filled) + side ∈ {2 close short, 4 close long}.
func (t *MEXCTrader) GetClosedPnL(startTime time.Time, limit int) ([]types.ClosedPnLRecord, error) {
	orders, err := t.fetchFilledHistory(startTime, limit)
	if err != nil {
		return nil, err
	}

	records := make([]types.ClosedPnLRecord, 0, len(orders))
	for _, o := range orders {
		// Only closing orders carry realized PnL.
		if o.Side != mexcSideCloseShort && o.Side != mexcSideCloseLong {
			continue
		}

		qty, err := t.contractsToQty(o.Symbol, int64(o.DealVol))
		if err != nil {
			qty = o.DealVol
		}

		side := "long"
		if o.Side == mexcSideCloseShort {
			side = "short"
		}

		records = append(records, types.ClosedPnLRecord{
			Symbol:      o.Symbol,
			Side:        side,
			ExitPrice:   o.DealAvgPrice,
			Quantity:    qty,
			RealizedPnL: o.Profit,
			Fee:         -(o.TakerFee + o.MakerFee),
			Leverage:    o.Leverage,
			EntryTime:   time.UnixMilli(o.CreateTime).UTC(),
			ExitTime:    time.UnixMilli(o.UpdateTime).UTC(),
			OrderID:     o.OrderID,
			CloseType:   "unknown",
		})
	}

	return records, nil
}
