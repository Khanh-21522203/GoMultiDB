package cdc

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func BenchmarkStoreAppendEvent(b *testing.B) {
	ctx := context.Background()
	for _, size := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("N%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				s := NewStore()
				for seq := 1; seq <= size; seq++ {
					if err := s.AppendEvent(ctx, Event{StreamID: "bench", TabletID: "tb", Sequence: uint64(seq), TimestampUTC: time.Now().UTC()}); err != nil {
						b.Fatalf("append: %v", err)
					}
				}
			}
		})
	}
}

func BenchmarkStorePoll(b *testing.B) {
	ctx := context.Background()
	s := NewStore()
	for seq := 1; seq <= 10000; seq++ {
		if err := s.AppendEvent(ctx, Event{StreamID: "bench", TabletID: "tb", Sequence: uint64(seq), TimestampUTC: time.Now().UTC()}); err != nil {
			b.Fatalf("append: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		after := uint64((i * 100) % 9000)
		if _, err := s.Poll(ctx, PollRequest{StreamID: "bench", TabletID: "tb", AfterSeq: after, MaxRecords: 100}); err != nil {
			b.Fatalf("poll: %v", err)
		}
	}
}
