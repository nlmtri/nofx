package mexc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"nofx/logger"
	"nofx/market"
	"nofx/store"
	"sort"
	"strconv"
	"strings"
	"time"
)

// MEXCTrade represents a filled order (MEXC state=3) mapped to NOFX order/fill shape.
// Granularity is per-order, not per-fill — MEXC /list/order_deals requires symbol,
// so we use /list/history_orders instead (symbol optional, aggregated fill data).
type MEXCTrade struct {
	Symbol      string
	OrderID     string    // MEXC orderId (used as both order + trade ID since 1:1)
	Side        string    // "BUY" or "SELL"
	FillPrice   float64   // dealAvgPrice
	FillQty     float64   // base coin quantity (from dealVol contracts)
	Fee         float64   // takerFee + makerFee (positive)
	FeeAsset    string    // feeCurrency
	ExecTime    time.Time // updateTime
	ProfitLoss  float64   // profit (non-zero only for closing orders)
	OrderType   string    // "MARKET" / "LIMIT"
	OrderAction string    // open_long / open_short / close_long / close_short
}

// mexcHistoryOrderRaw matches /list/history_orders row shape.
type mexcHistoryOrderRaw struct {
	OrderID      int64   `json:"orderId"`
	Symbol       string  `json:"symbol"`
	Side         int     `json:"side"` // 1=open long, 2=close short, 3=open short, 4=close long
	Price        float64 `json:"price"`
	Vol          float64 `json:"vol"`          // ordered contracts
	DealAvgPrice float64 `json:"dealAvgPrice"` // avg fill price
	DealVol      float64 `json:"dealVol"`      // filled contracts
	OrderType    int     `json:"orderType"`    // 1 Limit, 5 Market, ...
	Leverage     int     `json:"leverage"`
	TakerFee     float64 `json:"takerFee"`
	MakerFee     float64 `json:"makerFee"`
	Profit       float64 `json:"profit"`
	FeeCurrency  string  `json:"feeCurrency"`
	State        int     `json:"state"` // 3 = filled
	CreateTime   int64   `json:"createTime"`
	UpdateTime   int64   `json:"updateTime"`
	ExternalOID  string  `json:"externalOid"`
}

// fetchFilledHistory pages through /list/history_orders returning all filled orders.
func (t *MEXCTrader) fetchFilledHistory(startTime time.Time, limit int) ([]mexcHistoryOrderRaw, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	// MEXC history_orders uses camelCase for time params (startTime/endTime),
	// but snake_case for pagination (page_num/page_size). Mixed by design.
	params := url.Values{}
	params.Set("states", "3") // 3 = filled
	params.Set("startTime", strconv.FormatInt(startTime.UnixMilli(), 10))
	params.Set("endTime", strconv.FormatInt(time.Now().UnixMilli(), 10))
	params.Set("page_num", "1")
	params.Set("page_size", strconv.Itoa(limit))

	data, err := t.doRequest(http.MethodGet, mexcOrderListHistoryPath, params, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get history orders: %w", err)
	}

	// MEXC returns `resultList` wrapped or a raw array — try both.
	var wrapped struct {
		ResultList []mexcHistoryOrderRaw `json:"resultList"`
	}
	if err := json.Unmarshal(data, &wrapped); err == nil && wrapped.ResultList != nil {
		return wrapped.ResultList, nil
	}
	var direct []mexcHistoryOrderRaw
	if err := json.Unmarshal(data, &direct); err != nil {
		return nil, fmt.Errorf("failed to parse history orders: %w", err)
	}
	return direct, nil
}

// GetTrades retrieves filled-order "trades" over a window (per-order granularity).
func (t *MEXCTrader) GetTrades(startTime time.Time, limit int) ([]MEXCTrade, error) {
	orders, err := t.fetchFilledHistory(startTime, limit)
	if err != nil {
		return nil, err
	}

	trades := make([]MEXCTrade, 0, len(orders))
	for _, o := range orders {
		if o.DealVol == 0 {
			continue
		}

		qty, err := t.contractsToQty(o.Symbol, int64(o.DealVol))
		if err != nil {
			qty = o.DealVol
		}

		var side, action string
		switch o.Side {
		case mexcSideOpenLong:
			side, action = "BUY", "open_long"
		case mexcSideCloseShort:
			side, action = "BUY", "close_short"
		case mexcSideOpenShort:
			side, action = "SELL", "open_short"
		case mexcSideCloseLong:
			side, action = "SELL", "close_long"
		default:
			continue
		}

		orderKind := "LIMIT"
		if o.OrderType == mexcOrderTypeMarket || o.OrderType == mexcOrderTypeClosePosition {
			orderKind = "MARKET"
		}

		trades = append(trades, MEXCTrade{
			Symbol:      o.Symbol,
			OrderID:     strconv.FormatInt(o.OrderID, 10),
			Side:        side,
			FillPrice:   o.DealAvgPrice,
			FillQty:     qty,
			Fee:         o.TakerFee + o.MakerFee,
			FeeAsset:    o.FeeCurrency,
			ExecTime:    time.UnixMilli(o.UpdateTime).UTC(),
			ProfitLoss:  o.Profit,
			OrderType:   orderKind,
			OrderAction: action,
		})
	}

	return trades, nil
}

// SyncOrdersFromMEXC syncs recent MEXC filled orders into local store.
func (t *MEXCTrader) SyncOrdersFromMEXC(traderID, exchangeID, exchangeType string, st *store.Store) error {
	if st == nil {
		return fmt.Errorf("store is nil")
	}

	startTime := time.Now().Add(-24 * time.Hour)
	logger.Infof("🔄 Syncing MEXC orders from: %s", startTime.Format(time.RFC3339))

	trades, err := t.GetTrades(startTime, 100)
	if err != nil {
		return fmt.Errorf("failed to get trades: %w", err)
	}
	logger.Infof("📥 Received %d filled orders from MEXC", len(trades))

	sort.Slice(trades, func(i, j int) bool {
		return trades[i].ExecTime.UnixMilli() < trades[j].ExecTime.UnixMilli()
	})

	orderStore := st.Order()
	positionStore := st.Position()
	posBuilder := store.NewPositionBuilder(positionStore)
	syncedCount := 0

	for _, tr := range trades {
		existing, err := orderStore.GetOrderByExchangeID(exchangeID, tr.OrderID)
		if err == nil && existing != nil {
			continue
		}

		symbol := market.Normalize(tr.Symbol)

		positionSide := "LONG"
		if strings.Contains(tr.OrderAction, "short") {
			positionSide = "SHORT"
		}

		execMs := tr.ExecTime.UTC().UnixMilli()
		orderRec := &store.TraderOrder{
			TraderID:        traderID,
			ExchangeID:      exchangeID,
			ExchangeType:    exchangeType,
			ExchangeOrderID: tr.OrderID,
			Symbol:          symbol,
			Side:            tr.Side,
			PositionSide:    "BOTH",
			Type:            tr.OrderType,
			OrderAction:     tr.OrderAction,
			Quantity:        tr.FillQty,
			Price:           tr.FillPrice,
			Status:          "FILLED",
			FilledQuantity:  tr.FillQty,
			AvgFillPrice:    tr.FillPrice,
			Commission:      -tr.Fee,
			FilledAt:        execMs,
			CreatedAt:       execMs,
			UpdatedAt:       execMs,
		}
		if err := orderStore.CreateOrder(orderRec); err != nil {
			logger.Infof("  ⚠️ Failed to sync MEXC order %s: %v", tr.OrderID, err)
			continue
		}

		fillRec := &store.TraderFill{
			TraderID:        traderID,
			ExchangeID:      exchangeID,
			ExchangeType:    exchangeType,
			OrderID:         orderRec.ID,
			ExchangeOrderID: tr.OrderID,
			ExchangeTradeID: tr.OrderID, // 1:1 with order when using history_orders
			Symbol:          symbol,
			Side:            tr.Side,
			Price:           tr.FillPrice,
			Quantity:        tr.FillQty,
			QuoteQuantity:   tr.FillPrice * tr.FillQty,
			Commission:      -tr.Fee,
			CommissionAsset: tr.FeeAsset,
			RealizedPnL:     tr.ProfitLoss,
			IsMaker:         false,
			CreatedAt:       execMs,
		}
		if err := orderStore.CreateFill(fillRec); err != nil {
			logger.Infof("  ⚠️ Failed to sync MEXC fill %s: %v", tr.OrderID, err)
		}

		if err := posBuilder.ProcessTrade(
			traderID, exchangeID, exchangeType,
			symbol, positionSide, tr.OrderAction,
			tr.FillQty, tr.FillPrice, -tr.Fee, tr.ProfitLoss,
			execMs, tr.OrderID,
		); err != nil {
			logger.Infof("  ⚠️ Failed to sync MEXC position for %s: %v", tr.OrderID, err)
		}

		syncedCount++
		logger.Infof("  ✅ Synced MEXC order: %s %s %s qty=%.6f price=%.6f pnl=%.2f action=%s",
			tr.OrderID, symbol, tr.Side, tr.FillQty, tr.FillPrice, tr.ProfitLoss, tr.OrderAction)
	}

	logger.Infof("✅ MEXC order sync completed: %d new orders", syncedCount)
	return nil
}

// StartOrderSync starts the background sync goroutine for MEXC.
func (t *MEXCTrader) StartOrderSync(traderID, exchangeID, exchangeType string, st *store.Store, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			if err := t.SyncOrdersFromMEXC(traderID, exchangeID, exchangeType, st); err != nil {
				logger.Infof("⚠️  MEXC order sync failed: %v", err)
			}
		}
	}()
	logger.Infof("🔄 MEXC order sync started (interval: %v)", interval)
}
