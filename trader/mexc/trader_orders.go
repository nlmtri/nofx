package mexc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"nofx/logger"
	"nofx/trader/types"
	"strconv"
	"strings"
	"time"
)

// MEXC order side codes (contract API v1). Critical — not intuitive:
//   1 = Open long  (buy to open)
//   2 = Close short (buy to close)
//   3 = Open short (sell to open)
//   4 = Close long (sell to close)
const (
	mexcSideOpenLong   = 1
	mexcSideCloseShort = 2
	mexcSideOpenShort  = 3
	mexcSideCloseLong  = 4
)

// MEXC order type codes.
//   1=Limit, 2=Post-only, 3=IOC, 4=FOK, 5=Market, 6=Close-position (market, reduces to zero)
const (
	mexcOrderTypeLimit         = 1
	mexcOrderTypeMarket        = 5
	mexcOrderTypeClosePosition = 6
)

// Plan order trigger types.
//   1 = Stop-loss, 2 = Take-profit
const (
	mexcTriggerTypeStopLoss   = 1
	mexcTriggerTypeTakeProfit = 2
)

// submitOrder is the unified order submission helper.
// leverage is only passed through on opening sides (docs: "leverage must be
// provided when opening a position"). Closes derive leverage from position.
// Returns orderID string.
func (t *MEXCTrader) submitOrder(symbol string, side, orderType int, vol int64, price float64, leverage int, extra map[string]interface{}) (string, error) {
	body := map[string]interface{}{
		"symbol":       symbol,
		"side":         side,
		"type":         orderType,
		"vol":          vol,
		"openType":     t.openType(), // 1 isolated, 2 cross (from SetMarginMode)
		"positionMode": 2,             // 2 = one-way, 1 = dual-side (hedge)
		"externalOid":  genMEXCExternalOID(),
	}
	// Leverage required on opens.
	if (side == mexcSideOpenLong || side == mexcSideOpenShort) && leverage > 0 {
		body["leverage"] = leverage
	}
	if price > 0 && orderType != mexcOrderTypeMarket && orderType != mexcOrderTypeClosePosition {
		body["price"] = price
	}
	for k, v := range extra {
		body[k] = v
	}

	data, err := t.doRequest(http.MethodPost, mexcOrderSubmitPath, nil, body)
	if err != nil {
		return "", err
	}

	// MEXC returns `data` as either a string (orderId) or an object.
	var asStr string
	if err := json.Unmarshal(data, &asStr); err == nil && asStr != "" {
		return asStr, nil
	}
	var asObj struct {
		OrderID string `json:"orderId"`
	}
	if err := json.Unmarshal(data, &asObj); err == nil && asObj.OrderID != "" {
		return asObj.OrderID, nil
	}
	return strings.Trim(string(data), `"`), nil
}

// OpenLong opens a market long position.
func (t *MEXCTrader) OpenLong(symbol string, quantity float64, leverage int) (map[string]interface{}, error) {
	sym := t.normalizeSymbol(symbol)
	t.CancelAllOrders(sym)
	if err := t.SetLeverage(sym, leverage); err != nil {
		logger.Infof("  ⚠️ [MEXC] set leverage failed: %v", err)
	}

	vol, err := t.qtyToContracts(sym, quantity)
	if err != nil || vol <= 0 {
		return nil, fmt.Errorf("invalid quantity → contracts (%f): %v", quantity, err)
	}

	logger.Infof("  📊 [MEXC] OpenLong: symbol=%s, vol=%d, leverage=%d", sym, vol, leverage)

	orderID, err := t.submitOrder(sym, mexcSideOpenLong, mexcOrderTypeMarket, vol, 0, leverage, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open long: %w", err)
	}
	t.clearCache()
	logger.Infof("✓ [MEXC] OpenLong success: %s orderId=%s", sym, orderID)

	return map[string]interface{}{
		"orderId": orderID,
		"symbol":  sym,
		"status":  "FILLED",
	}, nil
}

// OpenShort opens a market short position.
func (t *MEXCTrader) OpenShort(symbol string, quantity float64, leverage int) (map[string]interface{}, error) {
	sym := t.normalizeSymbol(symbol)
	t.CancelAllOrders(sym)
	if err := t.SetLeverage(sym, leverage); err != nil {
		logger.Infof("  ⚠️ [MEXC] set leverage failed: %v", err)
	}

	vol, err := t.qtyToContracts(sym, quantity)
	if err != nil || vol <= 0 {
		return nil, fmt.Errorf("invalid quantity → contracts (%f): %v", quantity, err)
	}

	logger.Infof("  📊 [MEXC] OpenShort: symbol=%s, vol=%d, leverage=%d", sym, vol, leverage)

	orderID, err := t.submitOrder(sym, mexcSideOpenShort, mexcOrderTypeMarket, vol, 0, leverage, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open short: %w", err)
	}
	t.clearCache()
	logger.Infof("✓ [MEXC] OpenShort success: %s orderId=%s", sym, orderID)

	return map[string]interface{}{
		"orderId": orderID,
		"symbol":  sym,
		"status":  "FILLED",
	}, nil
}

// resolveCloseVol derives vol (contracts) for closing: if quantity=0, query position.
func (t *MEXCTrader) resolveCloseVol(sym, wantSide string, quantity float64) (int64, error) {
	if quantity > 0 {
		return t.qtyToContracts(sym, quantity)
	}
	positions, err := t.GetPositions()
	if err != nil {
		return 0, err
	}
	for _, p := range positions {
		if p["symbol"] == sym && p["side"] == wantSide {
			qty, _ := p["positionAmt"].(float64)
			if qty > 0 {
				return t.qtyToContracts(sym, qty)
			}
		}
	}
	return 0, fmt.Errorf("%s %s position not found", wantSide, sym)
}

// CloseLong closes an existing long position.
func (t *MEXCTrader) CloseLong(symbol string, quantity float64) (map[string]interface{}, error) {
	sym := t.normalizeSymbol(symbol)
	vol, err := t.resolveCloseVol(sym, "long", quantity)
	if err != nil || vol <= 0 {
		return nil, fmt.Errorf("CloseLong: %v", err)
	}
	logger.Infof("  📊 [MEXC] CloseLong: symbol=%s vol=%d", sym, vol)
	orderID, err := t.submitOrder(sym, mexcSideCloseLong, mexcOrderTypeMarket, vol, 0, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to close long: %w", err)
	}
	t.clearCache()
	logger.Infof("✓ [MEXC] CloseLong success: %s orderId=%s", sym, orderID)
	return map[string]interface{}{"orderId": orderID, "symbol": sym, "status": "FILLED"}, nil
}

// CloseShort closes an existing short position.
func (t *MEXCTrader) CloseShort(symbol string, quantity float64) (map[string]interface{}, error) {
	sym := t.normalizeSymbol(symbol)
	vol, err := t.resolveCloseVol(sym, "short", quantity)
	if err != nil || vol <= 0 {
		return nil, fmt.Errorf("CloseShort: %v", err)
	}
	logger.Infof("  📊 [MEXC] CloseShort: symbol=%s vol=%d", sym, vol)
	orderID, err := t.submitOrder(sym, mexcSideCloseShort, mexcOrderTypeMarket, vol, 0, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to close short: %w", err)
	}
	t.clearCache()
	logger.Infof("✓ [MEXC] CloseShort success: %s orderId=%s", sym, orderID)
	return map[string]interface{}{"orderId": orderID, "symbol": sym, "status": "FILLED"}, nil
}

// placePlanOrder places a stop-loss/take-profit plan order.
// MEXC /planorder/place/v2 requires `leverage`; derive from current position, fall back to 10.
func (t *MEXCTrader) placePlanOrder(sym string, positionSide string, quantity, triggerPrice float64, triggerType int) error {
	vol, err := t.qtyToContracts(sym, quantity)
	if err != nil || vol <= 0 {
		return fmt.Errorf("plan order invalid qty (%f): %v", quantity, err)
	}

	// Closing side depends on which position it protects.
	closeSide := mexcSideCloseLong
	wantSide := "long"
	if strings.EqualFold(positionSide, "SHORT") {
		closeSide = mexcSideCloseShort
		wantSide = "short"
	}

	// Use leverage from current open position (MEXC requires this field on plan orders).
	leverage := 10
	if positions, err := t.GetPositions(); err == nil {
		for _, p := range positions {
			if p["symbol"] == sym && p["side"] == wantSide {
				if lev, ok := p["leverage"].(float64); ok && lev > 0 {
					leverage = int(lev)
				}
				break
			}
		}
	}

	body := map[string]interface{}{
		"symbol":       sym,
		"side":         closeSide,
		"openType":     t.openType(),
		"vol":          vol,
		"leverage":     leverage,
		"triggerPrice": triggerPrice,
		"triggerType":  triggerType,
		"executeCycle": 1,
		"orderType":    mexcOrderTypeMarket,
		"trend":        1, // latest price
	}
	_, err = t.doRequest(http.MethodPost, mexcPlanOrderPlacePath, nil, body)
	return err
}

// SetStopLoss places a stop-loss plan order.
func (t *MEXCTrader) SetStopLoss(symbol string, positionSide string, quantity, stopPrice float64) error {
	sym := t.normalizeSymbol(symbol)
	if err := t.placePlanOrder(sym, positionSide, quantity, stopPrice, mexcTriggerTypeStopLoss); err != nil {
		return fmt.Errorf("failed to set stop loss: %w", err)
	}
	logger.Infof("  ✓ [MEXC] Stop loss set: %s @ %.4f", sym, stopPrice)
	return nil
}

// SetTakeProfit places a take-profit plan order.
func (t *MEXCTrader) SetTakeProfit(symbol string, positionSide string, quantity, takeProfitPrice float64) error {
	sym := t.normalizeSymbol(symbol)
	if err := t.placePlanOrder(sym, positionSide, quantity, takeProfitPrice, mexcTriggerTypeTakeProfit); err != nil {
		return fmt.Errorf("failed to set take profit: %w", err)
	}
	logger.Infof("  ✓ [MEXC] Take profit set: %s @ %.4f", sym, takeProfitPrice)
	return nil
}

// listPlanOrders returns open plan orders filtered by triggerType (0 = all).
// MEXC requires start_time + end_time + page_num + page_size.
func (t *MEXCTrader) listPlanOrders(sym string, triggerType int) ([]mexcPlanOrder, error) {
	params := url.Values{}
	if sym != "" {
		params.Set("symbol", sym)
	}
	params.Set("states", "1") // 1 = untriggered
	params.Set("start_time", strconv.FormatInt(time.Now().Add(-7*24*time.Hour).UnixMilli(), 10))
	params.Set("end_time", strconv.FormatInt(time.Now().UnixMilli(), 10))
	params.Set("page_num", "1")
	params.Set("page_size", "100")

	data, err := t.doRequest(http.MethodGet, mexcPlanOrderListPath, params, nil)
	if err != nil {
		return nil, err
	}

	var all []mexcPlanOrder
	if err := json.Unmarshal(data, &all); err != nil {
		return nil, err
	}
	if triggerType == 0 {
		return all, nil
	}
	filtered := make([]mexcPlanOrder, 0, len(all))
	for _, p := range all {
		if p.TriggerType == triggerType {
			filtered = append(filtered, p)
		}
	}
	return filtered, nil
}

type mexcPlanOrder struct {
	ID           int64   `json:"id"`
	Symbol       string  `json:"symbol"`
	Side         int     `json:"side"`
	TriggerPrice float64 `json:"triggerPrice"`
	TriggerType  int     `json:"triggerType"`
	Vol          float64 `json:"vol"`
	State        int     `json:"state"`
}

// cancelPlanOrdersBy cancels plan orders in one batched POST (max 50 per call).
func (t *MEXCTrader) cancelPlanOrdersBy(orders []mexcPlanOrder) {
	const batchSize = 50
	for i := 0; i < len(orders); i += batchSize {
		end := i + batchSize
		if end > len(orders) {
			end = len(orders)
		}
		body := make([]map[string]interface{}, 0, end-i)
		for _, o := range orders[i:end] {
			body = append(body, map[string]interface{}{"symbol": o.Symbol, "orderId": o.ID})
		}
		if _, err := t.doRequest(http.MethodPost, mexcPlanOrderCancelPath, nil, body); err != nil {
			logger.Warnf("[MEXC] plan cancel batch failed: %v", err)
		}
	}
}

// CancelStopLossOrders cancels all SL plan orders for a symbol.
func (t *MEXCTrader) CancelStopLossOrders(symbol string) error {
	sym := t.normalizeSymbol(symbol)
	orders, err := t.listPlanOrders(sym, mexcTriggerTypeStopLoss)
	if err != nil {
		return err
	}
	t.cancelPlanOrdersBy(orders)
	return nil
}

// CancelTakeProfitOrders cancels all TP plan orders for a symbol.
func (t *MEXCTrader) CancelTakeProfitOrders(symbol string) error {
	sym := t.normalizeSymbol(symbol)
	orders, err := t.listPlanOrders(sym, mexcTriggerTypeTakeProfit)
	if err != nil {
		return err
	}
	t.cancelPlanOrdersBy(orders)
	return nil
}

// CancelStopOrders cancels both SL and TP plan orders.
func (t *MEXCTrader) CancelStopOrders(symbol string) error {
	t.CancelStopLossOrders(symbol)
	t.CancelTakeProfitOrders(symbol)
	return nil
}

// CancelAllOrders cancels all regular + plan orders for a symbol.
func (t *MEXCTrader) CancelAllOrders(symbol string) error {
	sym := t.normalizeSymbol(symbol)

	// Regular orders — body is a JSON object with `symbol` field per docs.
	body := map[string]interface{}{"symbol": sym}
	if _, err := t.doRequest(http.MethodPost, mexcOrderCancelAllPath, nil, body); err != nil {
		logger.Warnf("[MEXC] cancel regular failed %s: %v", sym, err)
	}

	// Plan orders (SL + TP)
	t.CancelStopOrders(sym)
	return nil
}

// GetOrderStatus queries a single order by ID.
func (t *MEXCTrader) GetOrderStatus(symbol string, orderID string) (map[string]interface{}, error) {
	// MEXC query path: /api/v1/private/order/get/{order_id}
	path := mexcOrderGetByIDPath + "/" + orderID
	data, err := t.doRequest(http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get order: %w", err)
	}

	var order struct {
		OrderID      string  `json:"orderId"`
		Symbol       string  `json:"symbol"`
		State        int     `json:"state"` // 1=uninformed, 2=uncompleted, 3=completed, 4=cancelled, 5=invalid
		Price        float64 `json:"price"`
		DealAvgPrice float64 `json:"dealAvgPrice"`
		Vol          float64 `json:"vol"`
		DealVol      float64 `json:"dealVol"`
		Side         int     `json:"side"`
		OrderType    int     `json:"orderType"`
		Category     int     `json:"category"`
		Profit       float64 `json:"profit"`
		FeeRate      float64 `json:"feeRate"`
		Fee          float64 `json:"takerFee"`
		CreateTime   int64   `json:"createTime"`
		UpdateTime   int64   `json:"updateTime"`
	}
	if err := json.Unmarshal(data, &order); err != nil {
		return nil, err
	}

	// Convert contracts → qty for executed size.
	execQty, _ := t.contractsToQty(order.Symbol, int64(order.DealVol))

	statusMap := map[int]string{
		1: "NEW",
		2: "PARTIALLY_FILLED",
		3: "FILLED",
		4: "CANCELED",
		5: "CANCELED",
	}
	status, ok := statusMap[order.State]
	if !ok {
		status = strconv.Itoa(order.State)
	}

	return map[string]interface{}{
		"orderId":     order.OrderID,
		"symbol":      order.Symbol,
		"status":      status,
		"avgPrice":    order.DealAvgPrice,
		"executedQty": execQty,
		"side":        order.Side,
		"type":        order.OrderType,
		"time":        order.CreateTime,
		"updateTime":  order.UpdateTime,
		"commission":  -order.Fee,
	}, nil
}

// GetOpenOrders returns pending regular + plan orders.
func (t *MEXCTrader) GetOpenOrders(symbol string) ([]types.OpenOrder, error) {
	sym := t.normalizeSymbol(symbol)
	var result []types.OpenOrder

	// 1. Regular orders — MEXC requires page_num + page_size.
	params := url.Values{}
	if sym != "" {
		params.Set("symbol", sym)
	}
	params.Set("page_num", "1")
	params.Set("page_size", "100")
	data, err := t.doRequest(http.MethodGet, mexcOrderListOpenPath, params, nil)
	if err == nil && data != nil {
		var orders []struct {
			OrderID   string  `json:"orderId"`
			Symbol    string  `json:"symbol"`
			Side      int     `json:"side"`
			OrderType int     `json:"orderType"`
			Price     float64 `json:"price"`
			Vol       float64 `json:"vol"`
		}
		if err := json.Unmarshal(data, &orders); err == nil {
			for _, o := range orders {
				qty, _ := t.contractsToQty(o.Symbol, int64(o.Vol))
				side, pside := mexcSideToPositionSide(o.Side)

				orderKind := "LIMIT"
				if o.OrderType == mexcOrderTypeMarket || o.OrderType == mexcOrderTypeClosePosition {
					orderKind = "MARKET"
				}

				result = append(result, types.OpenOrder{
					OrderID:      o.OrderID,
					Symbol:       o.Symbol,
					Side:         side,
					PositionSide: pside,
					Type:         orderKind,
					Price:        o.Price,
					Quantity:     qty,
					Status:       "NEW",
				})
			}
		}
	} else if err != nil {
		logger.Warnf("[MEXC] GetOpenOrders regular failed: %v", err)
	}

	// 2. Plan orders (SL/TP)
	plans, err := t.listPlanOrders(sym, 0)
	if err != nil {
		logger.Warnf("[MEXC] GetOpenOrders plan failed: %v", err)
	} else {
		for _, p := range plans {
			qty, _ := t.contractsToQty(p.Symbol, int64(p.Vol))
			side, pside := mexcSideToPositionSide(p.Side)

			kind := "STOP_MARKET"
			if p.TriggerType == mexcTriggerTypeTakeProfit {
				kind = "TAKE_PROFIT_MARKET"
			}
			result = append(result, types.OpenOrder{
				OrderID:      strconv.FormatInt(p.ID, 10),
				Symbol:       p.Symbol,
				Side:         side,
				PositionSide: pside,
				Type:         kind,
				StopPrice:    p.TriggerPrice,
				Quantity:     qty,
				Status:       "NEW",
			})
		}
	}

	logger.Infof("✓ [MEXC] GetOpenOrders: found %d open orders for %s", len(result), sym)
	return result, nil
}

// mexcSideToPositionSide maps MEXC numeric side → (Side, PositionSide) strings.
func mexcSideToPositionSide(side int) (string, string) {
	switch side {
	case mexcSideOpenLong:
		return "BUY", "LONG"
	case mexcSideCloseShort:
		return "BUY", "SHORT"
	case mexcSideOpenShort:
		return "SELL", "SHORT"
	case mexcSideCloseLong:
		return "SELL", "LONG"
	}
	return "", ""
}
