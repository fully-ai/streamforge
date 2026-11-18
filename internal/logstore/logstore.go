package logstore

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	recordMagic uint16 = 0x5346 // "SF"
	flagGzip    byte   = 0x01
	headerLen          = 35 // magic(2)+crc(4)+offset(8)+ts(8)+flags(1)+klen(4)+vlen(4)+hlen(4)
	indexEntry         = 16 // relOffset(4)+pos(8)+tsMs(4)
)

var ErrCorruptRecord = errors.New("corrupt log record CRC mismatch")

// Record represents one durable message stored in a segmented commit log.
type Record struct {
	Offset     uint64            `json:"offset"`
	Timestamp  int64             `json:"timestamp_ns"`
	Key        []byte            `json:"key,omitempty"`
	Value      []byte            `json:"value"`
	Headers    map[string]string `json:"headers,omitempty"`
	Compressed bool              `json:"compressed,omitempty"`
}

// Segment manages one append-only `.log` file and a paired sparse `.index` file.
type Segment struct {
	baseOffset uint64
	dir        string
	logFile    *os.File
	idxFile    *os.File
	size       int64
	nextOffset uint64
	lastIndexP int64
	createdAt  time.Time
}

func openSegment(dir string, baseOffset uint64) (*Segment, error) {
	stem := fmt.Sprintf("%020d", baseOffset)
	logPath := filepath.Join(dir, stem+".log")
	idxPath := filepath.Join(dir, stem+".index")

	lf, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}
	ixf, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		_ = lf.Close()
		return nil, err
	}
	info, err := lf.Stat()
	if err != nil {
		_ = lf.Close()
		_ = ixf.Close()
		return nil, err
	}

	s := &Segment{
		baseOffset: baseOffset,
		dir:        dir,
		logFile:    lf,
		idxFile:    ixf,
		size:       info.Size(),
		nextOffset: baseOffset,
		createdAt:  info.ModTime().UTC(),
	}
	if err := s.recoverTail(); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// recoverTail scans the segment verifying CRC32 frames and truncating any partial crash tail.
func (s *Segment) recoverTail() error {
	if _, err := s.logFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var validBytes int64
	var lastOff = s.baseOffset
	hasRecords := false

	hdr := make([]byte, headerLen)
	for {
		if _, err := io.ReadFull(s.logFile, hdr); err != nil {
			break
		}
		if binary.BigEndian.Uint16(hdr[0:2]) != recordMagic {
			break
		}
		expectedCRC := binary.BigEndian.Uint32(hdr[2:6])
		off := binary.BigEndian.Uint64(hdr[6:14])
		klen := int(binary.BigEndian.Uint32(hdr[23:27]))
		vlen := int(binary.BigEndian.Uint32(hdr[27:31]))
		hlen := int(binary.BigEndian.Uint32(hdr[31:35]))
		payloadLen := klen + vlen + hlen
		if payloadLen < 0 || payloadLen > 32*1024*1024 {
			break
		}
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(s.logFile, payload); err != nil {
			// Truncated crash tail: stop cleanly at validBytes
			break
		}
		h := crc32.NewIEEE()
		_, _ = h.Write(hdr[6:headerLen])
		_, _ = h.Write(payload)
		if h.Sum32() != expectedCRC {
			// Truncate corrupt tail safely if it happened on incomplete crash write
			break
		}
		validBytes += int64(headerLen + payloadLen)
		lastOff = off
		hasRecords = true
	}

	if validBytes < s.size {
		if err := s.logFile.Truncate(validBytes); err != nil {
			return err
		}
		s.size = validBytes
	}
	if hasRecords {
		s.nextOffset = lastOff + 1
	} else {
		s.nextOffset = s.baseOffset
	}
	_, err := s.logFile.Seek(s.size, io.SeekStart)
	return err
}

func (s *Segment) appendRecord(rec *Record, compress bool) error {
	val := rec.Value
	var flags byte
	if compress && len(val) > 64 {
		var gzBuf bytes.Buffer
		zw := gzip.NewWriter(&gzBuf)
		_, _ = zw.Write(val)
		_ = zw.Close()
		if gzBuf.Len() < len(val) {
			val = gzBuf.Bytes()
			flags |= flagGzip
		}
	}

	var hdrBytes []byte
	if len(rec.Headers) > 0 {
		var parts []string
		for k, v := range rec.Headers {
			parts = append(parts, k+"="+v)
		}
		hdrBytes = []byte(strings.Join(parts, "\n"))
	}

	frame := make([]byte, headerLen+len(rec.Key)+len(val)+len(hdrBytes))
	binary.BigEndian.PutUint16(frame[0:2], recordMagic)
	binary.BigEndian.PutUint64(frame[6:14], rec.Offset)
	binary.BigEndian.PutUint64(frame[14:22], uint64(rec.Timestamp))
	frame[22] = flags
	binary.BigEndian.PutUint32(frame[23:27], uint32(len(rec.Key)))
	binary.BigEndian.PutUint32(frame[27:31], uint32(len(val)))
	binary.BigEndian.PutUint32(frame[31:35], uint32(len(hdrBytes)))
	copy(frame[headerLen:], rec.Key)
	copy(frame[headerLen+len(rec.Key):], val)
	copy(frame[headerLen+len(rec.Key)+len(val):], hdrBytes)

	crc := crc32.ChecksumIEEE(frame[6:])
	binary.BigEndian.PutUint32(frame[2:6], crc)

	pos := s.size
	if _, err := s.logFile.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	if _, err := s.logFile.Write(frame); err != nil {
		return err
	}
	s.size += int64(len(frame))
	s.nextOffset = rec.Offset + 1

	// Write sparse index entry every 4KB
	if s.size-s.lastIndexP >= 4096 || pos == 0 {
		var ix [indexEntry]byte
		binary.BigEndian.PutUint32(ix[0:4], uint32(rec.Offset-s.baseOffset))
		binary.BigEndian.PutUint64(ix[4:12], uint64(pos))
		binary.BigEndian.PutUint32(ix[12:16], uint32(rec.Timestamp/1_000_000))
		_, _ = s.idxFile.Write(ix[:])
		s.lastIndexP = s.size
	}
	return nil
}

func (s *Segment) readFrom(startOffset uint64, maxRecords int) ([]Record, error) {
	seekPos := s.lookupIndexPos(startOffset)
	if _, err := s.logFile.Seek(seekPos, io.SeekStart); err != nil {
		return nil, err
	}
	var out []Record
	hdr := make([]byte, headerLen)
	for len(out) < maxRecords {
		if _, err := io.ReadFull(s.logFile, hdr); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return out, err
		}
		if binary.BigEndian.Uint16(hdr[0:2]) != recordMagic {
			return out, ErrCorruptRecord
		}
		expectedCRC := binary.BigEndian.Uint32(hdr[2:6])
		off := binary.BigEndian.Uint64(hdr[6:14])
		ts := int64(binary.BigEndian.Uint64(hdr[14:22]))
		flags := hdr[22]
		klen := int(binary.BigEndian.Uint32(hdr[23:27]))
		vlen := int(binary.BigEndian.Uint32(hdr[27:31]))
		hlen := int(binary.BigEndian.Uint32(hdr[31:35]))
		payload := make([]byte, klen+vlen+hlen)
		if _, err := io.ReadFull(s.logFile, payload); err != nil {
			break
		}

		h := crc32.NewIEEE()
		_, _ = h.Write(hdr[6:headerLen])
		_, _ = h.Write(payload)
		if h.Sum32() != expectedCRC {
			return out, ErrCorruptRecord
		}
		if off < startOffset {
			continue
		}
		key := append([]byte(nil), payload[:klen]...)
		val := payload[klen : klen+vlen]
		compressed := flags&flagGzip != 0
		if compressed {
			zr, err := gzip.NewReader(bytes.NewReader(val))
			if err == nil {
				dec, err := io.ReadAll(zr)
				_ = zr.Close()
				if err == nil {
					val = dec
				}
			}
		} else {
			val = append([]byte(nil), val...)
		}
		var headers map[string]string
		if hlen > 0 {
			headers = make(map[string]string)
			for _, line := range strings.Split(string(payload[klen+vlen:]), "\n") {
				if k, v, ok := strings.Cut(line, "="); ok {
					headers[k] = v
				}
			}
		}
		out = append(out, Record{
			Offset:     off,
			Timestamp:  ts,
			Key:        key,
			Value:      val,
			Headers:    headers,
			Compressed: compressed,
		})
	}
	return out, nil
}

func (s *Segment) lookupIndexPos(targetOffset uint64) int64 {
	if _, err := s.idxFile.Seek(0, io.SeekStart); err != nil {
		return 0
	}
	var best int64
	var entry [indexEntry]byte
	for {
		if _, err := io.ReadFull(s.idxFile, entry[:]); err != nil {
			break
		}
		rel := binary.BigEndian.Uint32(entry[0:4])
		pos := int64(binary.BigEndian.Uint64(entry[4:12]))
		if s.baseOffset+uint64(rel) <= targetOffset {
			best = pos
		} else {
			break
		}
	}
	return best
}

func (s *Segment) Close() error {
	var err error
	if s.logFile != nil {
		err = s.logFile.Close()
	}
	if s.idxFile != nil {
		if e2 := s.idxFile.Close(); err == nil {
			err = e2
		}
	}
	return err
}

// Config configures a PartitionLog's segment rotation and durability.
type Config struct {
	MaxSegmentBytes int64
	SyncOnAppend    bool
	CompressBatch   bool
}

// PartitionLog manages an ordered chain of append-only segments for one topic partition.
type PartitionLog struct {
	mu       sync.RWMutex
	dir      string
	cfg      Config
	segments []*Segment
}

// Open opens or recovers an ordered PartitionLog directory.
func Open(dir string, cfg Config) (*PartitionLog, error) {
	if cfg.MaxSegmentBytes <= 0 {
		cfg.MaxSegmentBytes = 1024 * 1024
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var baseOffsets []uint64
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".log") {
			stem := strings.TrimSuffix(e.Name(), ".log")
			if off, err := strconv.ParseUint(stem, 10, 64); err == nil {
				baseOffsets = append(baseOffsets, off)
			}
		}
	}
	sort.Slice(baseOffsets, func(i, j int) bool { return baseOffsets[i] < baseOffsets[j] })
	if len(baseOffsets) == 0 {
		baseOffsets = []uint64{0}
	}

	var segs []*Segment
	for _, bo := range baseOffsets {
		seg, err := openSegment(dir, bo)
		if err != nil {
			return nil, err
		}
		segs = append(segs, seg)
	}
	return &PartitionLog{dir: dir, cfg: cfg, segments: segs}, nil
}

// Append writes a batch of records atomically to the active segment, rolling segments cleanly.
func (l *PartitionLog) Append(recs []Record) ([]uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	offsets := make([]uint64, 0, len(recs))
	now := time.Now().UTC().UnixNano()
	for i := range recs {
		active := l.segments[len(l.segments)-1]
		if active.size >= l.cfg.MaxSegmentBytes {
			nextBase := active.nextOffset
			newSeg, err := openSegment(l.dir, nextBase)
			if err != nil {
				return offsets, err
			}
			l.segments = append(l.segments, newSeg)
			active = newSeg
		}
		r := recs[i]
		r.Offset = active.nextOffset
		if r.Timestamp == 0 {
			r.Timestamp = now
		}
		if err := active.appendRecord(&r, l.cfg.CompressBatch); err != nil {
			return offsets, err
		}
		offsets = append(offsets, r.Offset)
	}
	if l.cfg.SyncOnAppend {
		_ = l.segments[len(l.segments)-1].logFile.Sync()
	}
	return offsets, nil
}

// Read reads up to maxRecords starting from offset across relevant segments.
func (l *PartitionLog) Read(offset uint64, maxRecords int) ([]Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if maxRecords <= 0 {
		maxRecords = 100
	}
	var out []Record
	for i, seg := range l.segments {
		if i+1 < len(l.segments) && l.segments[i+1].baseOffset <= offset {
			continue
		}
		batch, err := seg.readFrom(offset, maxRecords-len(out))
		if err != nil {
			return out, err
		}
		out = append(out, batch...)
		if len(out) >= maxRecords {
			break
		}
	}
	return out, nil
}

// HighWatermark returns the next offset to be assigned (LogEndOffset).
func (l *PartitionLog) LogEndOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[len(l.segments)-1].nextOffset
}

// SegmentCount returns active + sealed segment count.
func (l *PartitionLog) SegmentCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.segments)
}

// EnforceRetention deletes oldest sealed segments exceeding maxBytes (always keeping active segment).
func (l *PartitionLog) EnforceRetention(maxBytes int64) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if maxBytes <= 0 || len(l.segments) <= 1 {
		return 0, nil
	}
	var total int64
	for _, s := range l.segments {
		total += s.size
	}
	deleted := 0
	for len(l.segments) > 1 && total > maxBytes {
		victim := l.segments[0]
		total -= victim.size
		_ = victim.Close()
		stem := fmt.Sprintf("%020d", victim.baseOffset)
		_ = os.Remove(filepath.Join(l.dir, stem+".log"))
		_ = os.Remove(filepath.Join(l.dir, stem+".index"))
		l.segments = l.segments[1:]
		deleted++
	}
	return deleted, nil
}

// CompactKeys compacts closed segments by retaining only the latest offset per non-empty key.
func (l *PartitionLog) CompactKeys() (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.segments) <= 1 {
		return 0, nil
	}
	latest := make(map[string]Record)
	var keyless []Record
	removed := 0
	sealed := l.segments[:len(l.segments)-1]
	for _, seg := range sealed {
		recs, err := seg.readFrom(seg.baseOffset, 1000000)
		if err != nil {
			return 0, err
		}
		for _, r := range recs {
			if len(r.Key) == 0 {
				keyless = append(keyless, r)
				continue
			}
			k := string(r.Key)
			if _, dup := latest[k]; dup {
				removed++
			}
			latest[k] = r
		}
	}
	if removed == 0 {
		return 0, nil
	}
	var compacted []Record
	compacted = append(compacted, keyless...)
	for _, r := range latest {
		if len(r.Value) > 0 { // Drop tombstones
			compacted = append(compacted, r)
		} else {
			removed++
		}
	}
	sort.Slice(compacted, func(i, j int) bool { return compacted[i].Offset < compacted[j].Offset })

	// Rewrite sealed base segment cleanly
	base := sealed[0].baseOffset
	for _, seg := range sealed {
		_ = seg.Close()
		stem := fmt.Sprintf("%020d", seg.baseOffset)
		_ = os.Remove(filepath.Join(l.dir, stem+".log"))
		_ = os.Remove(filepath.Join(l.dir, stem+".index"))
	}
	newSealed, err := openSegment(l.dir, base)
	if err != nil {
		return removed, err
	}
	for i := range compacted {
		_ = newSealed.appendRecord(&compacted[i], l.cfg.CompressBatch)
	}
	active := l.segments[len(l.segments)-1]
	l.segments = []*Segment{newSealed, active}
	return removed, nil
}

func (l *PartitionLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.segments {
		_ = s.Close()
	}
	return nil
}
