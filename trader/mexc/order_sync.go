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

// MEXCTrade represents a single fill record derived from MEXC deals endpoint.
type MEXCTrade struct {
	Symbol      string
	TradeID     string
	OrderID     string
	Side        string // "BUY" or "SELL"
	FillPrice   float64
	FillQty     float64 // base coin quantity (already converted from contracts)
	Fee         float64
	FeeAsset    string
	ExecTime    time.Time
	ProfitLoss  float64
	OrderType   string
	OrderAction string // open_long / open_short / close_long / close_short
}

// mexcDealRaw matches MEXC /api/v1/private/order/deals row shape.
type mexcDealRaw struct {
	ID          int64   `json:"id"`
	Symbol      string  `json:"symbol"`
	OrderID     string  `json:"orderId"`
	Side        int     `json:"side"` // 1=open long, 2=close short, 3=open short, 4=close long
	Price       float64 `json:"price"`
	Vol         float64 `json:"vol"` // contracts
	Fee         float64 `json:"fee"`
	FeeCurrency string  `json:"feeCurrency"`
	Profit      float64 `json:"profit"`
	Taker       bool    `json:"taker"`
	Timestamp   int64   `json:"timestamp"`
}

// GetTrades retrieves recent deal/fill records from MEXC.
func (t *MEXCTrader) GetTrades(startTime time.Time, limit int) ([]MEXCTrade, error) {
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

	var deals []mexcDealRaw
	if err := json.Unmarshal(data, &deals); err != nil {
		return nil, fmt.Errorf("failed to parse deals: %w", err)
	}

	trades := make([]MEXCTrade, 0, len(deals))
	for _, d := range deals {
		qty, err := t.contractsToQty(d.Symbol, int64(d.Vol))
		if err != nil {
			qty = d.Vol
		}

		var side, action string
		switch d.Side {
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

		trades = append(trades, MEXCTrade{
			Symbol:      d.Symbol,
			TradeID:     strconv.FormatInt(d.ID, 10),
			OrderID:     d.OrderID,
			Side:        side,
			FillPrice:   d.Price,
			FillQty:     qty,
			Fee:         -d.Fee,
			FeeAsset:    d.FeeCurrency,
			ExecTime:    time.UnixMilli(d.Timestamp).UTC(),
			ProfitLoss:  d.Profit,
			OrderType:   "MARKET",
			OrderAction: action,
		})
	}

	return trades, nil
}

// SyncOrdersFromMEXC syncs recent MEXC fills into local store.
func (t *MEXCTrader) SyncOrdersFromMEXC(traderID, exchangeID, exchangeType string, st *store.Store) error {
	if st == nil {
		return fmt.Errorf("store is nil")
	}

	startTime := time.Now().Add(-24 * time.Hour)
	logger.Infof("🔄 Syncing MEXC trades from: %s", startTime.Format(time.RFC3339))

	trades, err := t.GetTrades(startTime, 100)
	if err != nil {
		return fmt.Errorf("failed to get trades: %w", err)
	}
	logger.Infof("📥 Received %d trades from MEXC", len(trades))

	sort.Slice(trades, func(i, j int) bool {
		return trades[i].ExecTime.UnixMilli() < trades[j].ExecTime.UnixMilli()
	})

	orderStore := st.Order()
	positionStore := st.Position()
	posBuilder := store.NewPositionBuilder(positionStore)
	syncedCount := 0

	for _, tr := range trades {
		existing, err := orderStore.GetOrderByExchangeID(exchangeID, tr.TradeID)
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
			ExchangeOrderID: tr.TradeID,
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
			Commission:      tr.Fee,
			FilledAt:        execMs,
			CreatedAt:       execMs,
			UpdatedAt:       execMs,
		}
		if err := orderStore.CreateOrder(orderRec); err != nil {
			logger.Infof("  ⚠️ Failed to sync MEXC trade %s: %v", tr.TradeID, err)
			continue
		}

		fillRec := &store.TraderFill{
			TraderID:        traderID,
			ExchangeID:      exchangeID,
			ExchangeType:    exchangeType,
			OrderID:         orderRec.ID,
			ExchangeOrderID: tr.OrderID,
			ExchangeTradeID: tr.TradeID,
			Symbol:          symbol,
			Side:            tr.Side,
			Price:           tr.FillPrice,
			Quantity:        tr.FillQty,
			QuoteQuantity:   tr.FillPrice * tr.FillQty,
			Commission:      tr.Fee,
			CommissionAsset: tr.FeeAsset,
			RealizedPnL:     tr.ProfitLoss,
			IsMaker:         false,
			CreatedAt:       execMs,
		}
		if err := orderStore.CreateFill(fillRec); err != nil {
			logger.Infof("  ⚠️ Failed to sync MEXC fill %s: %v", tr.TradeID, err)
		}

		if err := posBuilder.ProcessTrade(
			traderID, exchangeID, exchangeType,
			symbol, positionSide, tr.OrderAction,
			tr.FillQty, tr.FillPrice, tr.Fee, tr.ProfitLoss,
			execMs, tr.TradeID,
		); err != nil {
			logger.Infof("  ⚠️ Failed to sync MEXC position for %s: %v", tr.TradeID, err)
		}

		syncedCount++
		logger.Infof("  ✅ Synced MEXC trade: %s %s %s qty=%.6f price=%.6f pnl=%.2f action=%s",
			tr.TradeID, symbol, tr.Side, tr.FillQty, tr.FillPrice, tr.ProfitLoss, tr.OrderAction)
	}

	logger.Infof("✅ MEXC order sync completed: %d new trades", syncedCount)
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
