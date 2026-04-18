package wsoverride

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultMarketWSURL = "wss://fstream.binance.com/stream"
	writeWaitTimeout   = 10 * time.Second
	pongWaitTimeout    = 10 * time.Minute
	readLimitBytes     = 1 << 20 // 1MB per message
)

// MarketStreamClient maintains a single WS connection to Binance Futures
// combined market streams and populates PriceCache from incoming events.
type MarketStreamClient struct {
	URL    string
	Dialer *websocket.Dialer
	cache  *PriceCache

	mu         sync.Mutex // guards conn + subscribed + writer
	conn       *websocket.Conn
	subscribed map[string]struct{} // lowercase symbol set
	writeCh    chan []byte
	nextID     atomic.Int64

	connected atomic.Bool
	stopped   atomic.Bool
}

func NewMarketStreamClient(cache *PriceCache) *MarketStreamClient {
	return &MarketStreamClient{
		URL:        defaultMarketWSURL,
		Dialer:     websocket.DefaultDialer,
		cache:      cache,
		subscribed: map[string]struct{}{},
	}
}

// Start blocks until ctx is cancelled. It auto-reconnects with backoff.
func (c *MarketStreamClient) Start(ctx context.Context) error {
	bo := NewBackoff()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := c.runOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			d := bo.Next()
			log.Printf("⚠️ market WS error: %v (reconnect in %s)", err, d)
			if !Sleep(ctx, d) {
				return ctx.Err()
			}
			continue
		}
		bo.Reset()
	}
}

func (c *MarketStreamClient) runOnce(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// streams=  (empty initial — we SUBSCRIBE dynamically)
	conn, _, err := c.Dialer.DialContext(dialCtx, c.URL+"?streams=", http.Header{})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	log.Printf("✅ WS market stream connected")
	conn.SetReadLimit(readLimitBytes)
	_ = conn.SetReadDeadline(time.Now().Add(pongWaitTimeout))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(pongWaitTimeout))
		return nil
	})

	c.mu.Lock()
	c.conn = conn
	c.writeCh = make(chan []byte, 64)
	snapshot := make([]string, 0, len(c.subscribed))
	for s := range c.subscribed {
		snapshot = append(snapshot, s)
	}
	c.mu.Unlock()
	c.connected.Store(true)
	defer func() {
		c.connected.Store(false)
		_ = conn.Close()
	}()

	// Writer goroutine (single writer — gorilla forbids concurrent writes)
	writerDone := make(chan struct{})
	go c.writerLoop(ctx, conn, writerDone)

	// Re-subscribe existing symbols after reconnect
	for _, sym := range snapshot {
		c.sendSubscribe(sym)
	}

	// Reader loop (blocking)
	readErr := c.readerLoop(conn)
	close(c.writeCh)
	<-writerDone
	return readErr
}

func (c *MarketStreamClient) writerLoop(ctx context.Context, conn *websocket.Conn, done chan struct{}) {
	defer close(done)
	pingTicker := time.NewTicker(3 * time.Minute)
	defer pingTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-c.writeCh:
			if !ok {
				return
			}
			_ = conn.SetWriteDeadline(time.Now().Add(writeWaitTimeout))
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Printf("⚠️ market WS write error: %v", err)
				return
			}
		case <-pingTicker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(writeWaitTimeout))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *MarketStreamClient) readerLoop(conn *websocket.Conn) error {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		c.handleMessage(data)
	}
}

// combined stream payload: {"stream":"btcusdt@ticker","data":{...}}
type combinedFrame struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

func (c *MarketStreamClient) handleMessage(data []byte) {
	var frame combinedFrame
	if err := json.Unmarshal(data, &frame); err != nil || frame.Stream == "" {
		// Could be a subscription response ACK {"result":null,"id":N} — ignore silently.
		return
	}
	// suffix after '@'
	idx := strings.Index(frame.Stream, "@")
	if idx < 0 {
		return
	}
	symLower := frame.Stream[:idx]
	tail := frame.Stream[idx+1:]
	symUpper := strings.ToUpper(symLower)

	switch {
	case tail == "ticker":
		c.handleTicker(symUpper, frame.Data)
	case strings.HasPrefix(tail, "markPrice"):
		c.handleMarkPrice(symUpper, frame.Data)
	}
}

type rawTicker struct {
	E int64  `json:"E"`
	S string `json:"s"`
	C string `json:"c"` // lastPrice
}

func (c *MarketStreamClient) handleTicker(sym string, data json.RawMessage) {
	var t rawTicker
	if err := json.Unmarshal(data, &t); err != nil {
		return
	}
	lp, err := strconv.ParseFloat(t.C, 64)
	if err != nil {
		return
	}
	c.cache.SetTicker(&TickerSnapshot{
		Symbol:    sym,
		LastPrice: lp,
		EventTime: t.E,
		UpdatedAt: time.Now(),
	})
}

type rawMarkPrice struct {
	E int64  `json:"E"`
	S string `json:"s"`
	P string `json:"p"` // mark price
	I string `json:"i"` // index price
	R string `json:"r"` // funding rate
	T int64  `json:"T"` // next funding time
}

func (c *MarketStreamClient) handleMarkPrice(sym string, data json.RawMessage) {
	var m rawMarkPrice
	if err := json.Unmarshal(data, &m); err != nil {
		return
	}
	mp, _ := strconv.ParseFloat(m.P, 64)
	ip, _ := strconv.ParseFloat(m.I, 64)
	fr, _ := strconv.ParseFloat(m.R, 64)
	c.cache.SetMarkPrice(&MarkPriceSnapshot{
		Symbol:          sym,
		MarkPrice:       mp,
		IndexPrice:      ip,
		LastFundingRate: fr,
		NextFundingTime: m.T,
		EventTime:       m.E,
		UpdatedAt:       time.Now(),
	})
}

// EnsureSubscribed is idempotent — it ensures both ticker and markPrice@1s
// streams are subscribed for the given symbol (case-insensitive).
func (c *MarketStreamClient) EnsureSubscribed(symbol string) {
	if symbol == "" {
		return
	}
	sl := strings.ToLower(symbol)
	c.mu.Lock()
	if _, ok := c.subscribed[sl]; ok {
		c.mu.Unlock()
		return
	}
	c.subscribed[sl] = struct{}{}
	c.mu.Unlock()
	if c.connected.Load() {
		c.sendSubscribe(sl)
	}
}

type subRequest struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	ID     int64    `json:"id"`
}

func (c *MarketStreamClient) sendSubscribe(symLower string) {
	id := c.nextID.Add(1)
	req := subRequest{
		Method: "SUBSCRIBE",
		Params: []string{symLower + "@ticker", symLower + "@markPrice@1s"},
		ID:     id,
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return
	}
	c.mu.Lock()
	ch := c.writeCh
	c.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- payload:
	default:
		log.Printf("⚠️ subscribe queue full, dropping %s", symLower)
	}
}

// Close marks the client stopped. The Start loop exits when ctx is cancelled.
func (c *MarketStreamClient) Close() error {
	c.stopped.Store(true)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
