package wsoverride

import (
	"testing"
	"time"
)

func TestPriceCache_TickerRoundTrip(t *testing.T) {
	c := NewPriceCache()
	if _, ok := c.GetTicker("BTCUSDT"); ok {
		t.Fatal("empty cache returned hit")
	}
	c.SetTicker(&TickerSnapshot{Symbol: "BTCUSDT", LastPrice: 42000.5, UpdatedAt: time.Now()})
	s, ok := c.GetTicker("BTCUSDT")
	if !ok || s.LastPrice != 42000.5 {
		t.Fatalf("ticker mismatch: ok=%v snap=%+v", ok, s)
	}
}

func TestPriceCache_MarkPriceRoundTrip(t *testing.T) {
	c := NewPriceCache()
	c.SetMarkPrice(&MarkPriceSnapshot{
		Symbol:          "ETHUSDT",
		MarkPrice:       3500.2,
		LastFundingRate: 0.0001,
		NextFundingTime: 1713440000000,
		UpdatedAt:       time.Now(),
	})
	s, ok := c.GetMarkPrice("ETHUSDT")
	if !ok || s.MarkPrice != 3500.2 || s.LastFundingRate != 0.0001 {
		t.Fatalf("markPrice mismatch: ok=%v snap=%+v", ok, s)
	}
}

func TestUserStateCache_RoundTrip(t *testing.T) {
	c := NewUserStateCache()
	if _, ok := c.GetAccount("u1"); ok {
		t.Fatal("empty should miss")
	}
	c.SetAccount("u1", &AccountSnapshot{TotalWalletBalance: 100, UpdatedAt: time.Now()})
	a, ok := c.GetAccount("u1")
	if !ok || a.TotalWalletBalance != 100 {
		t.Fatalf("account mismatch: %+v", a)
	}
	// Cross-user isolation
	if _, ok := c.GetAccount("u2"); ok {
		t.Fatal("u2 must not see u1 data")
	}
}
