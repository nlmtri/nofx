package mexc

import (
	"testing"
	"time"
)

func TestSign(t *testing.T) {
	tr := &MEXCTrader{apiKey: "mx0vglsgdd7flAZvbj", secretKey: "0816968fd9d946c9a1dcd32c65b9eb1e3b13b4f0e7a04c77bcecd07e5d5c7c30"}
	ts := "1609913965097"
	payload := "symbol=BTC_USDT"
	sig := tr.sign(ts, payload)
	if len(sig) != 64 {
		t.Fatalf("expected 64-char hex signature, got %d: %s", len(sig), sig)
	}
	// Re-sign should be deterministic.
	if tr.sign(ts, payload) != sig {
		t.Fatal("signature not deterministic")
	}
}

func TestBuildSortedQuery(t *testing.T) {
	params := map[string][]string{
		"symbol": {"BTC_USDT"},
		"limit":  {"10"},
		"a":      {"1"},
	}
	got := buildSortedQuery(params)
	want := "a=1&limit=10&symbol=BTC_USDT"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestNormalizeSymbol(t *testing.T) {
	tr := &MEXCTrader{}
	cases := map[string]string{
		"BTCUSDT":   "BTC_USDT",
		"BTC_USDT":  "BTC_USDT",
		"btc_usdt":  "BTC_USDT",
		"ETHUSDT":   "ETH_USDT",
		"1000PEPEUSDT": "1000PEPE_USDT",
	}
	for in, want := range cases {
		if got := tr.normalizeSymbol(in); got != want {
			t.Errorf("normalizeSymbol(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestQtyContractsRoundTrip(t *testing.T) {
	tr := &MEXCTrader{
		contractsCache: map[string]*MEXCContract{
			"BTC_USDT": {Symbol: "BTC_USDT", ContractSize: 0.0001, VolScale: 4},
			"ETH_USDT": {Symbol: "ETH_USDT", ContractSize: 0.01, VolScale: 2},
		},
		contractsCacheTime: time.Now(),
	}
	// 0.1 BTC @ 0.0001 per contract = 1000 contracts
	contracts, err := tr.qtyToContracts("BTC_USDT", 0.1)
	if err != nil {
		t.Fatalf("qtyToContracts: %v", err)
	}
	if contracts != 1000 {
		t.Errorf("got %d contracts, want 1000", contracts)
	}
	qty, err := tr.contractsToQty("BTC_USDT", contracts)
	if err != nil {
		t.Fatalf("contractsToQty: %v", err)
	}
	if qty < 0.0999 || qty > 0.1001 {
		t.Errorf("round-trip qty=%f, want ~0.1", qty)
	}

	// Rounding: 0.15 ETH @ 0.01 = 15 contracts
	c2, _ := tr.qtyToContracts("ETH_USDT", 0.15)
	if c2 != 15 {
		t.Errorf("ETH got %d, want 15", c2)
	}
}

func TestFormatQuantity(t *testing.T) {
	tr := &MEXCTrader{
		contractsCache: map[string]*MEXCContract{
			"BTC_USDT": {Symbol: "BTC_USDT", ContractSize: 0.0001, VolScale: 4},
		},
		contractsCacheTime: time.Now(),
	}
	got, _ := tr.FormatQuantity("BTC_USDT", 0.12345678)
	if got != "0.1235" {
		t.Errorf("got %q, want 0.1235", got)
	}
}
