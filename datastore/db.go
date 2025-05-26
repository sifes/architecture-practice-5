package datastore

import (
	"bufio"
	"errors"
	"fmt"
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
	outFileName            = "current-data"
	segmentFilePrefix      = "segment-"
	defaultMaxSegmentSize  = 10 * 1024 * 1024 // 10MB
	mergeInterval          = 30 * time.Second
)

// Data types
const (
	TypeString uint8 = 1
	TypeInt64  uint8 = 2
)

var ErrNotFound = fmt.Errorf("record does not exist")
var ErrTypeMismatch = fmt.Errorf("value type does not match expected type")

type segmentInfo struct {
	id       int
	filePath string
	readOnly bool
}

type indexEntry struct {
	segmentID int
	offset    int64
}

type hashIndex map[string]indexEntry

type putRequest struct {
	key        string
	value      string
	int64Value int64
	valueType  uint8
	result     chan error
}

type Db struct {
	// Index synchronization - separate from file operations
	indexMu sync.RWMutex
	index   hashIndex
	
	// Database configuration
	dir            string
	maxSegmentSize int64
	
	// Active segment info (needs separate protection for reads)
	segmentMu       sync.RWMutex
	activeSegmentID int
	segments        []segmentInfo
	
	// Writer goroutine communication
	putChan    chan putRequest
	stopWriter chan struct{}
	writerWG   sync.WaitGroup
	
	// Merge control
	mergeChan chan struct{}
	stopMerge chan struct{}
	mergeWG   sync.WaitGroup
	
	// Writer goroutine state
	out       *os.File
	outOffset int64
}

type mergeRequest struct {
	result chan error
}

func Open(dir string) (*Db, error) {
	return OpenWithMaxSegmentSize(dir, defaultMaxSegmentSize)
}

func OpenWithMaxSegmentSize(dir string, maxSegmentSize int64) (*Db, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	db := &Db{
		dir:            dir,
		maxSegmentSize: maxSegmentSize,
		index:          make(hashIndex),
		putChan:        make(chan putRequest, 100), // Buffered channel for better performance
		stopWriter:     make(chan struct{}),
		mergeChan:      make(chan struct{}, 1),
		stopMerge:      make(chan struct{}),
	}

	// Load existing segments
	err = db.loadExistingSegments()
	if err != nil {
		return nil, err
	}

	// Create or open active segment
	err = db.openActiveSegment()
	if err != nil {
		return nil, err
	}

	// Rebuild index from all segments
	err = db.rebuildIndex()
	if err != nil {
		return nil, err
	}

	// Start writer goroutine
	db.writerWG.Add(1)
	go db.writerLoop()

	// Start background merge process
	db.mergeWG.Add(1)
	go db.mergeLoop()

	return db, nil
}

func (db *Db) loadExistingSegments() error {
	entries, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}

	var segmentInfos []segmentInfo
	maxSegmentID := -1
	hasCurrentData := false

	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, segmentFilePrefix) {
			// Extract segment ID
			idStr := strings.TrimPrefix(name, segmentFilePrefix)
			if id, err := strconv.Atoi(idStr); err == nil {
				segmentInfos = append(segmentInfos, segmentInfo{
					id:       id,
					filePath: filepath.Join(db.dir, name),
					readOnly: true,
				})
				if id > maxSegmentID {
					maxSegmentID = id
				}
			}
		} else if name == outFileName {
			hasCurrentData = true
		}
	}

	// Sort segments by ID (not by name)
	sort.Slice(segmentInfos, func(i, j int) bool {
		return segmentInfos[i].id < segmentInfos[j].id
	})

	// Update segments list and active segment ID
	db.segmentMu.Lock()
	db.segments = segmentInfos
	// Set active segment ID
	if hasCurrentData {
		db.activeSegmentID = maxSegmentID + 1
	} else {
		if maxSegmentID >= 0 {
			db.activeSegmentID = maxSegmentID + 1
		} else {
			db.activeSegmentID = 0
		}
	}
	db.segmentMu.Unlock()

	return nil
}

func (db *Db) openActiveSegment() error {
	outputPath := filepath.Join(db.dir, outFileName)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	// Get current size
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}

	db.out = f
	db.outOffset = stat.Size()

	return nil
}

func (db *Db) rebuildIndex() error {
	// Create a list of all segments including active segment
	type segmentToIndex struct {
		id       int
		filePath string
	}
	
	var allSegments []segmentToIndex
	
	// Get segments info safely
	db.segmentMu.RLock()
	// Add read-only segments
	for _, seg := range db.segments {
		allSegments = append(allSegments, segmentToIndex{
			id:       seg.id,
			filePath: seg.filePath,
		})
	}
	
	// Add active segment if exists
	activeID := db.activeSegmentID
	db.segmentMu.RUnlock()
	
	if db.out != nil {
		allSegments = append(allSegments, segmentToIndex{
			id:       activeID,
			filePath: filepath.Join(db.dir, outFileName),
		})
	}
	
	// Sort by segment ID (older segments first, newer segments last)
	sort.Slice(allSegments, func(i, j int) bool {
		return allSegments[i].id < allSegments[j].id
	})
	
	// Build new index
	newIndex := make(hashIndex)
	
	// Index segments in order - newer entries will override older ones
	for _, seg := range allSegments {
		err := db.indexSegmentFile(seg.filePath, seg.id, newIndex)
		if err != nil {
			return fmt.Errorf("failed to index segment %d (%s): %w", seg.id, seg.filePath, err)
		}
	}

	// Update index atomically
	db.indexMu.Lock()
	db.index = newIndex
	db.indexMu.Unlock()

	return nil
}

func (db *Db) indexSegmentFile(filePath string, segmentID int, index hashIndex) error {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Skip non-existent files
		}
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	offset := int64(0)

	for {
		var record entry
		n, err := record.DecodeFromReader(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if n != 0 {
					return fmt.Errorf("corrupted segment file %s", filePath)
				}
				break
			}
			return err
		}

		// Update index (latest entry wins)
		index[record.key] = indexEntry{
			segmentID: segmentID,
			offset:    offset,
		}
		
		offset += int64(n)
	}

	return nil
}

func (db *Db) writerLoop() {
	defer db.writerWG.Done()
	
	for {
		select {
		case <-db.stopWriter:
			return
			
		case req := <-db.putChan:
			err := db.handlePut(req)
			req.result <- err
		}
	}
}

func (db *Db) handlePut(req putRequest) error {
	// Handle special merge request
	if req.key == "__MERGE__" {
		return db.mergeSegments()
	}

	// Check if we need to rotate segment
	if db.outOffset >= db.maxSegmentSize {
		err := db.rotateActiveSegment()
		if err != nil {
			return err
		}
	}

	// Create entry based on type
	var e entry
	switch req.valueType {
	case TypeString:
		e = entry{
			key:         req.key,
			valueType:   TypeString,
			stringValue: req.value,
		}
	case TypeInt64:
		e = entry{
			key:        req.key,
			valueType:  TypeInt64,
			int64Value: req.int64Value,
		}
	default:
		return fmt.Errorf("unsupported value type: %d", req.valueType)
	}

	// Remember current offset for index
	currentOffset := db.outOffset

	// Write to active segment
	data := e.Encode()
	n, err := db.out.Write(data)
	if err != nil {
		return err
	}

	// Get current active segment ID for index update
	db.segmentMu.RLock()
	currentActiveID := db.activeSegmentID
	db.segmentMu.RUnlock()

	// Update index atomically
	db.indexMu.Lock()
	db.index[req.key] = indexEntry{
		segmentID: currentActiveID,
		offset:    currentOffset,
	}
	db.indexMu.Unlock()
	
	db.outOffset += int64(n)

	return nil
}

func (db *Db) rotateActiveSegment() error {
	if db.out == nil {
		return nil
	}

	// Close current active segment
	db.out.Close()

	// Get current active segment ID
	db.segmentMu.RLock()
	currentActiveID := db.activeSegmentID
	db.segmentMu.RUnlock()

	// Move to read-only segment
	oldPath := filepath.Join(db.dir, outFileName)
	newPath := filepath.Join(db.dir, fmt.Sprintf("%s%d", segmentFilePrefix, currentActiveID))
	
	err := os.Rename(oldPath, newPath)
	if err != nil {
		return err
	}

	// Update segments list and active ID
	db.segmentMu.Lock()
	db.segments = append(db.segments, segmentInfo{
		id:       currentActiveID,
		filePath: newPath,
		readOnly: true,
	})
	db.activeSegmentID++
	db.segmentMu.Unlock()

	// Create new active segment
	err = db.openActiveSegment()
	if err != nil {
		return err
	}

	// Trigger merge
	select {
	case db.mergeChan <- struct{}{}:
	default:
	}

	return nil
}

func (db *Db) Close() error {
	// Stop writer goroutine
	close(db.stopWriter)
	db.writerWG.Wait()

	// Stop merge process
	close(db.stopMerge)
	db.mergeWG.Wait()

	// Close active segment
	if db.out != nil {
		return db.out.Close()
	}
	
	return nil
}

func (db *Db) Get(key string) (string, error) {
	// Get index entry with read lock (non-blocking for other readers)
	db.indexMu.RLock()
	indexEntry, ok := db.index[key]
	db.indexMu.RUnlock()

	if !ok {
		return "", ErrNotFound
	}

	// Get current active segment ID safely
	db.segmentMu.RLock()
	currentActiveID := db.activeSegmentID
	db.segmentMu.RUnlock()

	// Determine which file to read from
	var filePath string
	if indexEntry.segmentID == currentActiveID {
		filePath = filepath.Join(db.dir, outFileName)
	} else {
		filePath = filepath.Join(db.dir, fmt.Sprintf("%s%d", segmentFilePrefix, indexEntry.segmentID))
	}

	// Open file for reading (each Get creates its own file descriptor)
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	record, err := db.readEntryFromFile(file, indexEntry.offset)
	if err != nil {
		return "", err
	}

	// Check if the stored value is a string
	if record.valueType != TypeString {
		return "", ErrTypeMismatch
	}

	return record.stringValue, nil
}

func (db *Db) GetInt64(key string) (int64, error) {
	// Get index entry with read lock (non-blocking for other readers)
	db.indexMu.RLock()
	indexEntry, ok := db.index[key]
	db.indexMu.RUnlock()

	if !ok {
		return 0, ErrNotFound
	}

	// Get current active segment ID safely
	db.segmentMu.RLock()
	currentActiveID := db.activeSegmentID
	db.segmentMu.RUnlock()

	// Determine which file to read from
	var filePath string
	if indexEntry.segmentID == currentActiveID {
		filePath = filepath.Join(db.dir, outFileName)
	} else {
		filePath = filepath.Join(db.dir, fmt.Sprintf("%s%d", segmentFilePrefix, indexEntry.segmentID))
	}

	// Open file for reading (each Get creates its own file descriptor)
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	record, err := db.readEntryFromFile(file, indexEntry.offset)
	if err != nil {
		return 0, err
	}

	// Check if the stored value is an int64
	if record.valueType != TypeInt64 {
		return 0, ErrTypeMismatch
	}

	return record.int64Value, nil
}

func (db *Db) readEntryFromFile(file *os.File, offset int64) (*entry, error) {
	// Seek to position
	_, err := file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	// Read entry
	var record entry
	_, err = record.DecodeFromReader(bufio.NewReader(file))
	if err != nil {
		return nil, err
	}

	return &record, nil
}

func (db *Db) Put(key, value string) error {
	// Send request to writer goroutine
	req := putRequest{
		key:       key,
		value:     value,
		valueType: TypeString,
		result:    make(chan error),
	}
	
	db.putChan <- req
	return <-req.result
}

func (db *Db) PutInt64(key string, value int64) error {
	// Send request to writer goroutine
	req := putRequest{
		key:        key,
		int64Value: value,
		valueType:  TypeInt64,
		result:     make(chan error),
	}
	
	db.putChan <- req
	return <-req.result
}

func (db *Db) Size() (int64, error) {
	// Get current segment list
	db.segmentMu.RLock()
	segments := make([]segmentInfo, len(db.segments))
	copy(segments, db.segments)
	db.segmentMu.RUnlock()

	var totalSize int64

	// Size of read-only segments
	for _, seg := range segments {
		stat, err := os.Stat(seg.filePath)
		if err != nil {
			if !os.IsNotExist(err) {
				return 0, err
			}
			continue
		}
		totalSize += stat.Size()
	}

	// Size of active segment
	activePath := filepath.Join(db.dir, outFileName)
	stat, err := os.Stat(activePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return 0, err
		}
	} else {
		totalSize += stat.Size()
	}

	return totalSize, nil
}

func (db *Db) mergeLoop() {
	defer db.mergeWG.Done()
	
	ticker := time.NewTicker(mergeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-db.stopMerge:
			return
		case <-ticker.C:
			db.tryMerge()
		case <-db.mergeChan:
			db.tryMerge()
		}
	}
}

func (db *Db) tryMerge() {
	// Check if merge is needed without blocking writers
	db.segmentMu.RLock()
	segmentCount := len(db.segments)
	db.segmentMu.RUnlock()

	// Need at least 3 read-only segments to merge (more conservative)
	if segmentCount < 3 {
		return
	}

	// Send merge request to writer goroutine to avoid concurrent modifications
	req := putRequest{
		key:       "__MERGE__",
		valueType: TypeString, // doesn't matter for merge
		result:    make(chan error),
	}
	
	select {
	case db.putChan <- req:
		err := <-req.result
		if err != nil {
			// Log error but don't crash (comment out for cleaner tests)
			// fmt.Printf("Merge failed: %v\n", err)
		}
	default:
		// Writer is busy, skip merge
	}
}

func (db *Db) mergeSegments() error {
	// Get segments to merge safely
	db.segmentMu.RLock()
	segmentsToMerge := make([]segmentInfo, len(db.segments))
	copy(segmentsToMerge, db.segments)
	db.segmentMu.RUnlock()

	// Collect all key-value pairs from read-only segments
	keyEntries := make(map[string]entry)
	
	// Process segments in order (oldest first, newest last)
	for _, seg := range segmentsToMerge {
		file, err := os.Open(seg.filePath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		
		reader := bufio.NewReader(file)
		for {
			var record entry
			_, err := record.DecodeFromReader(reader)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				file.Close()
				return err
			}
			// Keep latest entry for each key (preserves type and value)
			keyEntries[record.key] = record
		}
		file.Close()
	}

	if len(keyEntries) == 0 {
		return nil // Nothing to merge
	}

	// Create temporary merged file
	tempPath := filepath.Join(db.dir, "temp-merge")
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	// Write merged data
	for _, entryData := range keyEntries {
		data := entryData.Encode()
		
		_, err := tempFile.Write(data)
		if err != nil {
			tempFile.Close()
			os.Remove(tempPath)
			return err
		}
	}
	
	tempFile.Close()

	// Replace first segment with merged file
	mergedPath := segmentsToMerge[0].filePath
	
	// Remove old segments
	for _, seg := range segmentsToMerge {
		os.Remove(seg.filePath)
	}

	// Move temp file to merged location
	err = os.Rename(tempPath, mergedPath)
	if err != nil {
		return err
	}

	// Update segments list - keep only the merged segment
	db.segmentMu.Lock()
	db.segments = []segmentInfo{{
		id:       segmentsToMerge[0].id,
		filePath: mergedPath,
		readOnly: true,
	}}
	db.segmentMu.Unlock()

	// Rebuild index after merge to respect active segment
	// This ensures newer entries in active segment override merged ones
	err = db.rebuildIndex()
	if err != nil {
		return fmt.Errorf("failed to rebuild index after merge: %w", err)
	}

	return nil
}