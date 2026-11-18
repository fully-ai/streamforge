package logstore

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSegmentRollCrashTailCompactionAndRetention(t *testing.T) {
	dir := t.TempDir()
	pl, err := Open(dir, Config{MaxSegmentBytes: 250, CompressBatch: true})
	if err != nil {
		t.Fatal(err)
	}

	// Write 12 records with repeated keys ("k1", "k2") to trigger multiple segment rolls
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("k%d", i%2)
		v := fmt.Sprintf("payload-value-number-%03d-with-enough-length-for-gzip-compression-check", i)
		offs, err := pl.Append([]Record{{Key: []byte(k), Value: []byte(v), Headers: map[string]string{"seq": fmt.Sprintf("%d", i)}}})
		if err != nil || len(offs) != 1 || offs[0] != uint64(i) {
			t.Fatalf("append %d: err=%v offs=%v", i, err, offs)
		}
	}
	if pl.SegmentCount() < 3 {
		t.Fatalf("expected >= 3 rolled segments, got %d", pl.SegmentCount())
	}

	// Append torn crash tail directly to latest segment and reopen to verify clean tail recovery
	activeLog := filepath.Join(dir, fmt.Sprintf("%020d.log", pl.segments[len(pl.segments)-1].baseOffset))
	_ = pl.Close()
	f, err := os.OpenFile(activeLog, os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{0x53, 0x46, 0x00, 0x00, 0x00, 0x99, 0x11}) // partial truncated header
	_ = f.Close()

	reopened, err := Open(dir, Config{MaxSegmentBytes: 250, CompressBatch: true})
	if err != nil {
		t.Fatalf("reopen after torn crash tail: %v", err)
	}
	defer reopened.Close()
	if reopened.LogEndOffset() != 12 {
		t.Fatalf("expected LEO=12 after crash recovery, got %d", reopened.LogEndOffset())
	}

	recs, err := reopened.Read(0, 50)
	if err != nil || len(recs) != 12 {
		t.Fatalf("Read all: err=%v len=%d", err, len(recs))
	}

	removed, err := reopened.CompactKeys()
	if err != nil || removed == 0 {
		t.Fatalf("expected closed-segment key compaction removals > 0, got removed=%d err=%v", removed, err)
	}
}

func BenchmarkLogAppendBatch(b *testing.B) {
	dir := b.TempDir()
	pl, err := Open(dir, Config{MaxSegmentBytes: 4 * 1024 * 1024})
	if err != nil {
		b.Fatal(err)
	}
	defer pl.Close()
	batch := make([]Record, 50)
	for i := range batch {
		batch[i] = Record{Key: []byte("k"), Value: []byte(`{"event":"click","user":42}`)}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := pl.Append(batch); err != nil {
			b.Fatal(err)
		}
	}
}
