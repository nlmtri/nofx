package wsoverride

import (
	"testing"
)

func TestApplyAccountUpdate_BalanceMerge(t *testing.T) {
	cache := NewUserStateCache()
	cache.SetAccount("u1", &AccountSnapshot{
		Balances: map[string]AssetBalance{
			"USDT": {Asset: "USDT", WalletBalance: 100},
			"BNB":  {Asset: "BNB", WalletBalance: 2},
		},
		TotalWalletBalance: 100,
	})
	s := &UserDataStream{UserID: "u1", Cache: cache}
	raw := `{
	  "e":"ACCOUNT_UPDATE","E":1,"T":1,
	  "a":{
	    "m":"ORDER",
	    "B":[{"a":"USDT","wb":"250.5","cw":"250.5","bc":"150.5"}],
	    "P":[]
	  }
	}`
	if exp := s.handleEvent([]byte(raw)); exp {
		t.Fatal("should not be listenKeyExpired")
	}
	got, _ := cache.GetAccount("u1")
	if got.Balances["USDT"].WalletBalance != 250.5 {
		t.Fatalf("USDT not patched: %+v", got.Balances["USDT"])
	}
	if got.Balances["BNB"].WalletBalance != 2 {
		t.Fatalf("BNB should be preserved: %+v", got.Balances["BNB"])
	}
}

func TestApplyOrderUpdate_NewAndFill(t *testing.T) {
	cache := NewUserStateCache()
	cache.SetOpenOrders("u1", &OrdersSnapshot{Orders: []Order{}})
	s := &UserDataStream{UserID: "u1", Cache: cache}

	newOrder := `{
	  "e":"ORDER_TRADE_UPDATE","E":1,"T":1,
	  "o":{"s":"BTCUSDT","c":"client-1","S":"BUY","o":"LIMIT","f":"GTC","q":"0.1","p":"65000","ap":"0","sp":"0","x":"NEW","X":"NEW","i":111,"l":"0","z":"0","L":"0","ps":"LONG","T":10}
	}`
	s.handleEvent([]byte(newOrder))
	got, _ := cache.GetOpenOrders("u1")
	if len(got.Orders) != 1 || got.Orders[0].OrderID != 111 {
		t.Fatalf("expected 1 NEW order, got %+v", got.Orders)
	}

	// Same order FILLED → should be removed
	fillOrder := `{
	  "e":"ORDER_TRADE_UPDATE","E":2,"T":2,
	  "o":{"s":"BTCUSDT","c":"client-1","S":"BUY","o":"LIMIT","f":"GTC","q":"0.1","p":"65000","ap":"65000","sp":"0","x":"TRADE","X":"FILLED","i":111,"l":"0.1","z":"0.1","L":"65000","ps":"LONG","T":20}
	}`
	s.handleEvent([]byte(fillOrder))
	got, _ = cache.GetOpenOrders("u1")
	if len(got.Orders) != 0 {
		t.Fatalf("FILLED should remove order, got %+v", got.Orders)
	}
}

func TestHandleEvent_ListenKeyExpiredSignals(t *testing.T) {
	s := &UserDataStream{UserID: "u1", Cache: NewUserStateCache()}
	if !s.handleEvent([]byte(`{"e":"listenKeyExpired","E":1}`)) {
		t.Fatal("listenKeyExpired must signal reconnect")
	}
}

func TestHandleEvent_UnknownEventIgnored(t *testing.T) {
	s := &UserDataStream{UserID: "u1", Cache: NewUserStateCache()}
	if s.handleEvent([]byte(`{"e":"UNKNOWN","E":1}`)) {
		t.Fatal("unknown should not signal expiry")
	}
}
