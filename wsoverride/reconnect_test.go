package wsoverride

import (
	"context"
	"testing"
	"time"
)

func TestBackoff_Progression(t *testing.T) {
	b := NewBackoff()
	got := []time.Duration{b.Next(), b.Next(), b.Next(), b.Next()}
	// 1s, 2s, 4s, 8s
	want := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("step %d: got %s want %s", i, got[i], w)
		}
	}
}

func TestBackoff_CapAndReset(t *testing.T) {
	b := NewBackoff()
	var last time.Duration
	for i := 0; i < 20; i++ {
		last = b.Next()
	}
	if last != 60*time.Second {
		t.Fatalf("expected cap at 60s, got %s", last)
	}
	b.Reset()
	if got := b.Next(); got != 1*time.Second {
		t.Fatalf("after reset expected 1s, got %s", got)
	}
}

func TestSleep_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if Sleep(ctx, 5*time.Second) {
		t.Fatal("expected Sleep to return false on cancelled ctx")
	}
}
