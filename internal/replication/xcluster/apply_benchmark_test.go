package xcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"GoMultiDB/internal/replication/cdc"
)

type benchApplier struct{}

func (benchApplier) Apply(_ context.Context, _ cdc.Event) error { return nil }

func BenchmarkApplyEvent(b *testing.B) {
	ctx := context.Background()
	for _, size := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("N%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				store := cdc.NewStore()
				loop, err := NewLoop(Config{}, store, benchApplier{})
				if err != nil {
					b.Fatalf("new loop: %v", err)
				}
				for seq := 1; seq <= size; seq++ {
					ev := cdc.Event{StreamID: "bench", TabletID: "tb", Sequence: uint64(seq), TimestampUTC: time.Now().UTC()}
					if err := loop.ApplyEvent(ctx, ev); err != nil {
						b.Fatalf("apply: %v", err)
					}
				}
			}
		})
	}
}
