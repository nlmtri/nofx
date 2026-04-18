package wsoverride

import (
	"sync"
	"time"
)

// PriceCache stores public market snapshots keyed by upper-case symbol.
type PriceCache struct {
	tickers    sync.Map // symbol -> *TickerSnapshot
	markPrices sync.Map // symbol -> *MarkPriceSnapshot
}

func NewPriceCache() *PriceCache {
	return &PriceCache{}
}

type TickerSnapshot struct {
	Symbol    string
	LastPrice float64
	EventTime int64
	UpdatedAt time.Time
}

type MarkPriceSnapshot struct {
	Symbol          string
	MarkPrice       float64
	IndexPrice      float64
	EstSettlePrice  float64
	LastFundingRate float64
	NextFundingTime int64
	InterestRate    float64
	EventTime       int64
	UpdatedAt       time.Time
}

func (c *PriceCache) SetTicker(s *TickerSnapshot) {
	if s == nil || s.Symbol == "" {
		return
	}
	c.tickers.Store(s.Symbol, s)
}

func (c *PriceCache) GetTicker(symbol string) (*TickerSnapshot, bool) {
	v, ok := c.tickers.Load(symbol)
	if !ok {
		return nil, false
	}
	return v.(*TickerSnapshot), true
}

func (c *PriceCache) SetMarkPrice(s *MarkPriceSnapshot) {
	if s == nil || s.Symbol == "" {
		return
	}
	c.markPrices.Store(s.Symbol, s)
}

func (c *PriceCache) GetMarkPrice(symbol string) (*MarkPriceSnapshot, bool) {
	v, ok := c.markPrices.Load(symbol)
	if !ok {
		return nil, false
	}
	return v.(*MarkPriceSnapshot), true
}

// UserStateCache stores per-user account, position and order snapshots.
// Snapshots are immutable; mutation = replace.
type UserStateCache struct {
	accounts   sync.Map // userID -> *AccountSnapshot
	positions  sync.Map // userID -> *PositionsSnapshot
	openOrders sync.Map // userID -> *OrdersSnapshot
}

func NewUserStateCache() *UserStateCache {
	return &UserStateCache{}
}

type AccountSnapshot struct {
	// RawJSON from REST prime response -- preserved verbatim for schema fidelity.
	RawJSON    []byte
	Balances   map[string]AssetBalance
	TotalWalletBalance float64
	UpdatedAt  time.Time
	FromSource string // "rest-prime" | "ws-push"
}

type AssetBalance struct {
	Asset              string
	WalletBalance      float64
	CrossWalletBalance float64
	BalanceChange      float64
}

type PositionsSnapshot struct {
	RawJSON    []byte
	Positions  []Position
	UpdatedAt  time.Time
	FromSource string
}

type Position struct {
	Symbol           string
	PositionSide     string // BOTH | LONG | SHORT
	PositionAmt      float64
	EntryPrice       float64
	UnrealizedProfit float64
	MarginType       string
	IsolatedWallet   float64
}

type OrdersSnapshot struct {
	RawJSON    []byte
	Orders     []Order
	UpdatedAt  time.Time
	FromSource string
}

type Order struct {
	Symbol       string
	OrderID      int64
	ClientOrderID string
	Side         string
	PositionSide string
	Type         string
	Status       string
	Price        float64
	OrigQty      float64
	ExecutedQty  float64
	UpdateTime   int64
}

func (c *UserStateCache) GetAccount(userID string) (*AccountSnapshot, bool) {
	v, ok := c.accounts.Load(userID)
	if !ok {
		return nil, false
	}
	return v.(*AccountSnapshot), true
}

func (c *UserStateCache) SetAccount(userID string, s *AccountSnapshot) {
	if s == nil {
		return
	}
	c.accounts.Store(userID, s)
}

func (c *UserStateCache) GetPositions(userID string) (*PositionsSnapshot, bool) {
	v, ok := c.positions.Load(userID)
	if !ok {
		return nil, false
	}
	return v.(*PositionsSnapshot), true
}

func (c *UserStateCache) SetPositions(userID string, s *PositionsSnapshot) {
	if s == nil {
		return
	}
	c.positions.Store(userID, s)
}

func (c *UserStateCache) GetOpenOrders(userID string) (*OrdersSnapshot, bool) {
	v, ok := c.openOrders.Load(userID)
	if !ok {
		return nil, false
	}
	return v.(*OrdersSnapshot), true
}

func (c *UserStateCache) SetOpenOrders(userID string, s *OrdersSnapshot) {
	if s == nil {
		return
	}
	c.openOrders.Store(userID, s)
}
