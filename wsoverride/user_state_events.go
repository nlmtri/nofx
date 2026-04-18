package wsoverride

// Binance Futures User Data Stream event shapes.
// Schema ref: https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams

// Envelope common to every event.
type userEventEnvelope struct {
	Event     string `json:"e"` // ACCOUNT_UPDATE | ORDER_TRADE_UPDATE | MARGIN_CALL | listenKeyExpired | ...
	EventTime int64  `json:"E"`
}

// ACCOUNT_UPDATE:
//	{
//	  "e":"ACCOUNT_UPDATE","E":..., "T":...,
//	  "a":{ "m":"...", "B":[{asset,wb,cw,bc}], "P":[{s,pa,ep,cr,up,mt,iw,ps}] }
//	}
type accountUpdateEvent struct {
	Event     string `json:"e"`
	EventTime int64  `json:"E"`
	TxTime    int64  `json:"T"`
	A         struct {
		Reason  string             `json:"m"`
		Balance []accountBalanceEv `json:"B"`
		Position []accountPositionEv `json:"P"`
	} `json:"a"`
}

type accountBalanceEv struct {
	Asset              string `json:"a"`
	WalletBalance      string `json:"wb"`
	CrossWalletBalance string `json:"cw"`
	BalanceChange      string `json:"bc"`
}

type accountPositionEv struct {
	Symbol           string `json:"s"`
	PositionAmount   string `json:"pa"`
	EntryPrice       string `json:"ep"`
	AccumRealized    string `json:"cr"`
	UnrealizedProfit string `json:"up"`
	MarginType       string `json:"mt"` // isolated|cross
	IsolatedWallet   string `json:"iw"`
	PositionSide     string `json:"ps"` // BOTH|LONG|SHORT
}

// ORDER_TRADE_UPDATE:
//	{"e":"ORDER_TRADE_UPDATE","E":...,"T":...,"o":{...}}
type orderTradeUpdateEvent struct {
	Event     string  `json:"e"`
	EventTime int64   `json:"E"`
	TxTime    int64   `json:"T"`
	O         orderEv `json:"o"`
}

type orderEv struct {
	Symbol        string `json:"s"`
	ClientOrderID string `json:"c"`
	Side          string `json:"S"`
	Type          string `json:"o"`
	TimeInForce   string `json:"f"`
	OrigQty       string `json:"q"`
	Price         string `json:"p"`
	AvgPrice      string `json:"ap"`
	StopPrice     string `json:"sp"`
	ExecType      string `json:"x"`
	OrderStatus   string `json:"X"`
	OrderID       int64  `json:"i"`
	LastFilledQty string `json:"l"`
	CumFilledQty  string `json:"z"`
	LastFilledPx  string `json:"L"`
	PositionSide  string `json:"ps"`
	UpdateTime    int64  `json:"T"`
}

// listenKeyExpired: {"e":"listenKeyExpired","E":...}
// MARGIN_CALL: {"e":"MARGIN_CALL","E":...,"cw":"...","p":[...]}
