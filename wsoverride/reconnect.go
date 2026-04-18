package wsoverride

import (
	"context"
	"time"
)

// Backoff implements exponential backoff with a cap. Not safe for concurrent use.
type Backoff struct {
	Initial time.Duration
	Max     time.Duration
	Factor  float64
	current time.Duration
}

func NewBackoff() *Backoff {
	return &Backoff{
		Initial: 1 * time.Second,
		Max:     60 * time.Second,
		Factor:  2.0,
	}
}

// Next returns the duration to wait and advances the backoff.
func (b *Backoff) Next() time.Duration {
	if b.current == 0 {
		b.current = b.Initial
		return b.current
	}
	next := time.Duration(float64(b.current) * b.Factor)
	if next > b.Max {
		next = b.Max
	}
	b.current = next
	return b.current
}

// Reset resets backoff to zero (fresh start after successful connect).
func (b *Backoff) Reset() {
	b.current = 0
}

// Sleep waits for d or context cancellation; returns false if ctx cancelled.
func Sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
