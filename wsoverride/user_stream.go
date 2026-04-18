package wsoverride

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/gorilla/websocket"
)

const (
	defaultUserWSBase    = "wss://fstream.binance.com/ws/"
	listenKeyKeepaliveInterval = 30 * time.Minute
)

// UserDataStream keeps a per-user Binance listenKey + WS connection open and
// maintains a fresh snapshot of account/positions/orders in UserStateCache.
type UserDataStream struct {
	UserID string
	Client *futures.Client
	Cache  *UserStateCache

	WSBase string // override for tests; default wss://fstream.binance.com/ws/
	Dialer *websocket.Dialer

	mu         sync.Mutex
	listenKey  string
	conn       *websocket.Conn
	primed     atomic.Bool
	connected  atomic.Bool
	pendingBuf [][]byte // events buffered while REST prime in flight
}

// Healthy reports whether the stream is connected and REST prime has completed.
// When healthy, callers may serve cached snapshots without wall-clock freshness
// gating — WS deltas push any state change in near-real-time.
func (s *UserDataStream) Healthy() bool {
	return s.connected.Load() && s.primed.Load()
}

func NewUserDataStream(userID string, client *futures.Client, cache *UserStateCache) *UserDataStream {
	return &UserDataStream{
		UserID: userID,
		Client: client,
		Cache:  cache,
		WSBase: defaultUserWSBase,
		Dialer: websocket.DefaultDialer,
	}
}

// Start runs the loop: obtain listenKey -> prime cache -> connect WS -> read.
// Reconnects on error with backoff. Returns when ctx is cancelled.
func (s *UserDataStream) Start(ctx context.Context) error {
	bo := NewBackoff()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := s.runOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			d := bo.Next()
			log.Printf("⚠️ user WS[%s] error: %v (reconnect in %s)", s.userShort(), err, d)
			if !Sleep(ctx, d) {
				return ctx.Err()
			}
			continue
		}
		bo.Reset()
	}
}

func (s *UserDataStream) runOnce(ctx context.Context) error {
	key, err := s.obtainListenKey(ctx)
	if err != nil {
		return fmt.Errorf("obtain listenKey: %w", err)
	}
	s.mu.Lock()
	s.listenKey = key
	s.primed.Store(false)
	s.pendingBuf = nil
	s.mu.Unlock()

	// REST prime runs concurrently so WS can start reading early; events arriving
	// before prime completes get buffered.
	primeDone := make(chan struct{})
	go func() {
		defer close(primeDone)
		if err := s.primeCache(ctx); err != nil {
			log.Printf("⚠️ prime[%s] failed: %v", s.userShort(), err)
			return
		}
		s.primed.Store(true)
		s.flushPending()
	}()

	conn, _, err := s.Dialer.DialContext(ctx, s.WSBase+key, http.Header{})
	if err != nil {
		return fmt.Errorf("ws dial: %w", err)
	}
	s.connected.Store(true)
	defer s.connected.Store(false)
	log.Printf("✅ user data stream connected for %s", s.userShort())
	conn.SetReadLimit(readLimitBytes)
	_ = conn.SetReadDeadline(time.Now().Add(pongWaitTimeout))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(pongWaitTimeout))
		return nil
	})
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		s.conn = nil
		s.mu.Unlock()
	}()

	// Keepalive loop
	ctxLoop, cancel := context.WithCancel(ctx)
	defer cancel()
	go s.keepaliveLoop(ctxLoop, key)
	go s.pingLoop(ctxLoop, conn)

	// Read loop
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			// ensure prime goroutine finishes before return to avoid leak
			<-primeDone
			return fmt.Errorf("read: %w", err)
		}
		if !s.primed.Load() {
			s.bufferEvent(data)
			continue
		}
		if expired := s.handleEvent(data); expired {
			<-primeDone
			return fmt.Errorf("listenKeyExpired")
		}
	}
}

func (s *UserDataStream) bufferEvent(raw []byte) {
	cp := make([]byte, len(raw))
	copy(cp, raw)
	s.mu.Lock()
	s.pendingBuf = append(s.pendingBuf, cp)
	s.mu.Unlock()
}

func (s *UserDataStream) flushPending() {
	s.mu.Lock()
	buf := s.pendingBuf
	s.pendingBuf = nil
	s.mu.Unlock()
	for _, raw := range buf {
		_ = s.handleEvent(raw)
	}
}

func (s *UserDataStream) keepaliveLoop(ctx context.Context, key string) {
	t := time.NewTicker(listenKeyKeepaliveInterval)
	defer t.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.Client.NewKeepaliveUserStreamService().
				ListenKey(key).Do(ctx); err != nil {
				failures++
				log.Printf("⚠️ listenKey keepalive failed (%d/3) [%s]: %v", failures, s.userShort(), err)
				if failures >= 3 {
					// Force reconnect by closing the WS connection.
					s.mu.Lock()
					conn := s.conn
					s.mu.Unlock()
					if conn != nil {
						_ = conn.Close()
					}
					return
				}
				continue
			}
			failures = 0
		}
	}
}

func (s *UserDataStream) pingLoop(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(3 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = conn.SetWriteDeadline(time.Now().Add(writeWaitTimeout))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (s *UserDataStream) obtainListenKey(ctx context.Context) (string, error) {
	key, err := s.Client.NewStartUserStreamService().Do(ctx)
	if err != nil {
		return "", err
	}
	return key, nil
}

// primeCache performs one REST fetch of account + positions + openOrders and
// stores the raw JSON + parsed snapshot.
func (s *UserDataStream) primeCache(ctx context.Context) error {
	// Account
	acct, err := s.Client.NewGetAccountService().Do(ctx)
	if err != nil {
		return fmt.Errorf("GetAccount: %w", err)
	}
	raw, _ := json.Marshal(acct)
	balances := map[string]AssetBalance{}
	for _, a := range acct.Assets {
		wb, _ := strconv.ParseFloat(a.WalletBalance, 64)
		cw, _ := strconv.ParseFloat(a.CrossWalletBalance, 64)
		balances[a.Asset] = AssetBalance{
			Asset:              a.Asset,
			WalletBalance:      wb,
			CrossWalletBalance: cw,
		}
	}
	twb, _ := strconv.ParseFloat(acct.TotalWalletBalance, 64)
	s.Cache.SetAccount(s.UserID, &AccountSnapshot{
		RawJSON:            raw,
		Balances:           balances,
		TotalWalletBalance: twb,
		UpdatedAt:          time.Now(),
		FromSource:         "rest-prime",
	})

	// Positions
	posns, err := s.Client.NewGetPositionRiskService().Do(ctx)
	if err != nil {
		return fmt.Errorf("GetPositionRisk: %w", err)
	}
	rawPos, _ := json.Marshal(posns)
	positions := make([]Position, 0, len(posns))
	for _, p := range posns {
		pa, _ := strconv.ParseFloat(p.PositionAmt, 64)
		ep, _ := strconv.ParseFloat(p.EntryPrice, 64)
		up, _ := strconv.ParseFloat(p.UnRealizedProfit, 64)
		iw, _ := strconv.ParseFloat(p.IsolatedWallet, 64)
		positions = append(positions, Position{
			Symbol:           p.Symbol,
			PositionSide:     string(p.PositionSide),
			PositionAmt:      pa,
			EntryPrice:       ep,
			UnrealizedProfit: up,
			MarginType:       string(p.MarginType),
			IsolatedWallet:   iw,
		})
	}
	s.Cache.SetPositions(s.UserID, &PositionsSnapshot{
		RawJSON:    rawPos,
		Positions:  positions,
		UpdatedAt:  time.Now(),
		FromSource: "rest-prime",
	})

	// Open orders
	orders, err := s.Client.NewListOpenOrdersService().Do(ctx)
	if err != nil {
		return fmt.Errorf("ListOpenOrders: %w", err)
	}
	rawOrd, _ := json.Marshal(orders)
	ords := make([]Order, 0, len(orders))
	for _, o := range orders {
		pr, _ := strconv.ParseFloat(o.Price, 64)
		oq, _ := strconv.ParseFloat(o.OrigQuantity, 64)
		eq, _ := strconv.ParseFloat(o.ExecutedQuantity, 64)
		ords = append(ords, Order{
			Symbol:        o.Symbol,
			OrderID:       o.OrderID,
			ClientOrderID: o.ClientOrderID,
			Side:          string(o.Side),
			PositionSide:  string(o.PositionSide),
			Type:          string(o.Type),
			Status:        string(o.Status),
			Price:         pr,
			OrigQty:       oq,
			ExecutedQty:   eq,
			UpdateTime:    o.UpdateTime,
		})
	}
	s.Cache.SetOpenOrders(s.UserID, &OrdersSnapshot{
		RawJSON:    rawOrd,
		Orders:     ords,
		UpdatedAt:  time.Now(),
		FromSource: "rest-prime",
	})
	return nil
}

// handleEvent parses one raw WS message and applies it to the cache.
// Returns true if the listenKey expired and caller should reconnect.
func (s *UserDataStream) handleEvent(raw []byte) bool {
	var env userEventEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return false
	}
	switch env.Event {
	case "ACCOUNT_UPDATE":
		var e accountUpdateEvent
		if err := json.Unmarshal(raw, &e); err == nil {
			s.applyAccountUpdate(&e)
		}
	case "ORDER_TRADE_UPDATE":
		var e orderTradeUpdateEvent
		if err := json.Unmarshal(raw, &e); err == nil {
			s.applyOrderUpdate(&e)
		}
	case "MARGIN_CALL":
		log.Printf("⚠️ MARGIN_CALL[%s]: %s", s.userShort(), string(raw))
	case "listenKeyExpired":
		log.Printf("⚠️ listenKeyExpired[%s] — forcing reconnect", s.userShort())
		return true
	}
	return false
}

func (s *UserDataStream) applyAccountUpdate(e *accountUpdateEvent) {
	snap, ok := s.Cache.GetAccount(s.UserID)
	if !ok || snap == nil {
		snap = &AccountSnapshot{Balances: map[string]AssetBalance{}}
	}
	// copy-on-write
	balances := map[string]AssetBalance{}
	for k, v := range snap.Balances {
		balances[k] = v
	}
	for _, b := range e.A.Balance {
		wb, _ := strconv.ParseFloat(b.WalletBalance, 64)
		cw, _ := strconv.ParseFloat(b.CrossWalletBalance, 64)
		bc, _ := strconv.ParseFloat(b.BalanceChange, 64)
		balances[b.Asset] = AssetBalance{
			Asset:              b.Asset,
			WalletBalance:      wb,
			CrossWalletBalance: cw,
			BalanceChange:      bc,
		}
	}
	newAcct := &AccountSnapshot{
		RawJSON:    snap.RawJSON,
		Balances:   balances,
		TotalWalletBalance: snap.TotalWalletBalance,
		UpdatedAt:  time.Now(),
		FromSource: "ws-push",
	}
	s.Cache.SetAccount(s.UserID, newAcct)

	// Positions upsert by (symbol, positionSide)
	if len(e.A.Position) > 0 {
		posSnap, _ := s.Cache.GetPositions(s.UserID)
		var cur []Position
		if posSnap != nil {
			cur = append(cur, posSnap.Positions...)
		}
		for _, p := range e.A.Position {
			pa, _ := strconv.ParseFloat(p.PositionAmount, 64)
			ep, _ := strconv.ParseFloat(p.EntryPrice, 64)
			up, _ := strconv.ParseFloat(p.UnrealizedProfit, 64)
			iw, _ := strconv.ParseFloat(p.IsolatedWallet, 64)
			np := Position{
				Symbol:           p.Symbol,
				PositionSide:     p.PositionSide,
				PositionAmt:      pa,
				EntryPrice:       ep,
				UnrealizedProfit: up,
				MarginType:       p.MarginType,
				IsolatedWallet:   iw,
			}
			replaced := false
			for i, old := range cur {
				if old.Symbol == np.Symbol && old.PositionSide == np.PositionSide {
					cur[i] = np
					replaced = true
					break
				}
			}
			if !replaced {
				cur = append(cur, np)
			}
		}
		s.Cache.SetPositions(s.UserID, &PositionsSnapshot{
			RawJSON:    nilIfEmpty(posSnap),
			Positions:  cur,
			UpdatedAt:  time.Now(),
			FromSource: "ws-push",
		})
	}
}

func nilIfEmpty(p *PositionsSnapshot) []byte {
	if p == nil {
		return nil
	}
	return p.RawJSON
}

func (s *UserDataStream) applyOrderUpdate(e *orderTradeUpdateEvent) {
	snap, _ := s.Cache.GetOpenOrders(s.UserID)
	var cur []Order
	if snap != nil {
		cur = append(cur, snap.Orders...)
	}
	o := e.O
	pr, _ := strconv.ParseFloat(o.Price, 64)
	oq, _ := strconv.ParseFloat(o.OrigQty, 64)
	eq, _ := strconv.ParseFloat(o.CumFilledQty, 64)
	record := Order{
		Symbol:        o.Symbol,
		OrderID:       o.OrderID,
		ClientOrderID: o.ClientOrderID,
		Side:          o.Side,
		PositionSide:  o.PositionSide,
		Type:          o.Type,
		Status:        o.OrderStatus,
		Price:         pr,
		OrigQty:       oq,
		ExecutedQty:   eq,
		UpdateTime:    o.UpdateTime,
	}
	switch o.OrderStatus {
	case "NEW", "PARTIALLY_FILLED":
		replaced := false
		for i, old := range cur {
			if old.OrderID == record.OrderID {
				cur[i] = record
				replaced = true
				break
			}
		}
		if !replaced {
			cur = append(cur, record)
		}
	case "FILLED", "CANCELED", "EXPIRED", "REJECTED":
		filtered := cur[:0]
		for _, old := range cur {
			if old.OrderID != record.OrderID {
				filtered = append(filtered, old)
			}
		}
		cur = filtered
	}
	s.Cache.SetOpenOrders(s.UserID, &OrdersSnapshot{
		RawJSON:    nil,
		Orders:     cur,
		UpdatedAt:  time.Now(),
		FromSource: "ws-push",
	})
}

func (s *UserDataStream) userShort() string {
	if len(s.UserID) > 8 {
		return s.UserID[:8]
	}
	return s.UserID
}

// Close best-effort closes the current WS connection. The Start loop should be
// terminated by cancelling the context.
func (s *UserDataStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}
