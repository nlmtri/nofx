package wsoverride

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// stubTransport counts calls and returns a fixed response.
type stubTransport struct {
	called  int
	body    string
	status  int
}

func (s *stubTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	s.called++
	status := s.status
	if status == 0 {
		status = 200
	}
	body := s.body
	if body == "" {
		body = `{"ok":true}`
	}
	return &http.Response{
		StatusCode:    status,
		Header:        http.Header{"Content-Type": {"application/json"}},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}, nil
}

func mustReq(method, rawurl string) *http.Request {
	u, _ := url.Parse(rawurl)
	return &http.Request{Method: method, URL: u, Host: u.Host}
}

func TestPerUserTransport_TickerCacheHit(t *testing.T) {
	mc := NewPriceCache()
	mc.SetTicker(&TickerSnapshot{Symbol: "BTCUSDT", LastPrice: 42000.5, UpdatedAt: time.Now()})
	stub := &stubTransport{}
	tr := newPerUserTransport("u1", NewUserStateCache(), mc, stub)

	resp, err := tr.RoundTrip(mustReq("GET", "https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT"))
	if err != nil {
		t.Fatalf("roundtrip err: %v", err)
	}
	if stub.called != 0 {
		t.Fatalf("fallback should NOT be called on cache hit, got %d", stub.called)
	}
	body, _ := io.ReadAll(resp.Body)
	var p struct {
		Symbol string `json:"symbol"`
		Price  string `json:"price"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, body)
	}
	if p.Symbol != "BTCUSDT" || !strings.HasPrefix(p.Price, "42000") {
		t.Fatalf("bad response: %+v", p)
	}
}

func TestPerUserTransport_StaleFallsThrough(t *testing.T) {
	mc := NewPriceCache()
	mc.SetTicker(&TickerSnapshot{Symbol: "BTCUSDT", LastPrice: 1, UpdatedAt: time.Now().Add(-2 * time.Minute)})
	stub := &stubTransport{body: `{"symbol":"BTCUSDT","price":"1","time":0}`}
	tr := newPerUserTransport("u1", NewUserStateCache(), mc, stub)

	_, _ = tr.RoundTrip(mustReq("GET", "https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT"))
	if stub.called != 1 {
		t.Fatalf("expected fallthrough, got stub.called=%d", stub.called)
	}
}

func TestPerUserTransport_PostPassThrough(t *testing.T) {
	stub := &stubTransport{}
	tr := newPerUserTransport("u1", NewUserStateCache(), NewPriceCache(), stub)
	_, _ = tr.RoundTrip(mustReq("POST", "https://fapi.binance.com/fapi/v1/order"))
	if stub.called != 1 {
		t.Fatalf("POST must fall through, got %d", stub.called)
	}
}

func TestPerUserTransport_AccountHitFromRaw(t *testing.T) {
	uc := NewUserStateCache()
	raw := []byte(`{"totalWalletBalance":"100.00","assets":[{"asset":"USDT","walletBalance":"100.00","crossWalletBalance":"100.00"}]}`)
	uc.SetAccount("u1", &AccountSnapshot{RawJSON: raw, UpdatedAt: time.Now(), FromSource: "rest-prime"})
	stub := &stubTransport{}
	tr := newPerUserTransport("u1", uc, NewPriceCache(), stub)

	resp, err := tr.RoundTrip(mustReq("GET", "https://fapi.binance.com/fapi/v2/account"))
	if err != nil {
		t.Fatalf("roundtrip: %v", err)
	}
	if stub.called != 0 {
		t.Fatal("fallback must not be called on account cache hit")
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"totalWalletBalance"`) {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestPerUserTransport_OpenInterestCached(t *testing.T) {
	stub := &stubTransport{body: `{"symbol":"BTCUSDT","openInterest":"1234.5","time":0}`}
	tr := newPerUserTransport("u1", NewUserStateCache(), NewPriceCache(), stub)

	// First call: miss → stub called
	_, _ = tr.RoundTrip(mustReq("GET", "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"))
	// Second call same URL: should hit TTL cache
	_, _ = tr.RoundTrip(mustReq("GET", "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"))

	if stub.called != 1 {
		t.Fatalf("openInterest should be cached on second call, stub.called=%d", stub.called)
	}
}

func TestRenderOrders_FilterSymbol(t *testing.T) {
	snap := &OrdersSnapshot{
		Orders: []Order{
			{Symbol: "BTCUSDT", OrderID: 1, Price: 65000, OrigQty: 0.1, Status: "NEW"},
			{Symbol: "ETHUSDT", OrderID: 2, Price: 3500, OrigQty: 1, Status: "NEW"},
		},
	}
	body, err := renderOrders(snap, "BTCUSDT")
	if err != nil {
		t.Fatal(err)
	}
	var arr []map[string]any
	if err := json.Unmarshal(body, &arr); err != nil {
		t.Fatal(err)
	}
	if len(arr) != 1 || arr[0]["symbol"] != "BTCUSDT" {
		t.Fatalf("filter failed: %v", arr)
	}
}

func TestFormatFloat_TrimsZeros(t *testing.T) {
	cases := map[float64]string{
		42000.5:        "42000.5",
		0.0001:         "0.0001",
		65000:          "65000",
		3500.20000000:  "3500.2",
	}
	for in, want := range cases {
		if got := formatFloat(in); got != want {
			t.Errorf("formatFloat(%v)=%q want %q", in, got, want)
		}
	}
}
