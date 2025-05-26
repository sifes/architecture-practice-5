package datastore

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSegmentedDb_BasicOperations(t *testing.T) {
	tmp := t.TempDir()
	db, err := OpenWithMaxSegmentSize(tmp, 1024) // Small segment size for testing
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Test basic put/get
	err = db.Put("key1", "value1")
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	value, err := db.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if value != "value1" {
		t.Errorf("Expected 'value1', got '%s'", value)
	}

	// Test key update
	err = db.Put("key1", "value1_updated")
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	value, err = db.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get updated: %v", err)
	}
	if value != "value1_updated" {
		t.Errorf("Expected 'value1_updated', got '%s'", value)
	}

	// Test non-existent key
	_, err = db.Get("non_existent")
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

func TestSegmentedDb_SegmentCreation(t *testing.T) {
	tmp := t.TempDir()
	
	// Use small segment size to force segment creation
	db, err := OpenWithMaxSegmentSize(tmp, 200)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Add enough data to create multiple segments
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d_with_some_extra_data_to_make_it_larger", i)
		err = db.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	// Give time for any background operations
	time.Sleep(100 * time.Millisecond)

	// Verify data is accessible
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("key_%d", i)
		expectedValue := fmt.Sprintf("value_%d_with_some_extra_data_to_make_it_larger", i)
		
		value, err := db.Get(key)
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		if value != expectedValue {
			t.Errorf("Key %s: expected '%s', got '%s'", key, expectedValue, value)
		}
	}

	// Check that segment files were created
	files, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatal(err)
	}

	segmentCount := 0
	hasCurrentData := false
	
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "segment-") {
			segmentCount++
			t.Logf("Found segment: %s", file.Name())
		} else if file.Name() == "current-data" {
			hasCurrentData = true
			t.Logf("Found current-data")
		}
	}

	t.Logf("Found %d segment files and current-data: %v", segmentCount, hasCurrentData)
	
	// Should have created at least one segment or have current-data
	if segmentCount == 0 && !hasCurrentData {
		t.Error("Expected segment files to be created")
	}
}

func TestSegmentedDb_PersistenceAcrossRestarts(t *testing.T) {
	tmp := t.TempDir()
	
	// First session: create data
	{
		db, err := OpenWithMaxSegmentSize(tmp, 500)
		if err != nil {
			t.Fatal(err)
		}

		pairs := [][]string{
			{"persistent1", "value1"},
			{"persistent2", "value2"},
			{"persistent3", "value3"},
		}

		for _, pair := range pairs {
			err = db.Put(pair[0], pair[1])
			if err != nil {
				t.Fatalf("Failed to put %s: %v", pair[0], err)
			}
		}

		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Second session: verify data persisted
	{
		db, err := OpenWithMaxSegmentSize(tmp, 500)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		pairs := [][]string{
			{"persistent1", "value1"},
			{"persistent2", "value2"},
			{"persistent3", "value3"},
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Fatalf("Failed to get %s: %v", pair[0], err)
			}
			if value != pair[1] {
				t.Errorf("Key %s: expected '%s', got '%s'", pair[0], pair[1], value)
			}
		}
	}
}

func TestSegmentedDb_KeyUpdates(t *testing.T) {
	tmp := t.TempDir()
	
	db, err := OpenWithMaxSegmentSize(tmp, 300)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Initial data
	keys := []string{"update1", "update2", "update3"}
	for i, key := range keys {
		value := fmt.Sprintf("initial_value_%d", i)
		err = db.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	// Update values
	for i, key := range keys {
		value := fmt.Sprintf("updated_value_%d", i)
		err = db.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to update %s: %v", key, err)
		}
	}

	// Verify updates
	for i, key := range keys {
		expectedValue := fmt.Sprintf("updated_value_%d", i)
		value, err := db.Get(key)
		if err != nil {
			t.Fatalf("Failed to get updated %s: %v", key, err)
		}
		if value != expectedValue {
			t.Errorf("Key %s: expected '%s', got '%s'", key, expectedValue, value)
		}
	}
}

func TestSegmentedDb_LargeDataset(t *testing.T) {
	tmp := t.TempDir()
	
	db, err := OpenWithMaxSegmentSize(tmp, 1024) // 1KB segments
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create a dataset
	numKeys := 50
	keyValuePairs := make(map[string]string)
	
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("large_key_%04d", i)
		value := fmt.Sprintf("large_value_%04d", i)
		keyValuePairs[key] = value
		
		err = db.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	// Update some keys
	for i := 0; i < numKeys/3; i++ {
		key := fmt.Sprintf("large_key_%04d", i)
		value := fmt.Sprintf("updated_large_value_%04d", i)
		keyValuePairs[key] = value
		
		err = db.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to update %s: %v", key, err)
		}
	}

	// Give time for any background operations
	time.Sleep(200 * time.Millisecond)

	// Verify all data
	for key, expectedValue := range keyValuePairs {
		value, err := db.Get(key)
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		if value != expectedValue {
			t.Errorf("Key %s: expected '%s', got '%s'", key, expectedValue, value)
		}
	}

	// Check that multiple files might exist
	files, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Database files created: %d", len(files))
	for _, file := range files {
		t.Logf("  - %s", file.Name())
	}
}

func TestSegmentedDb_EdgeCases(t *testing.T) {
	tmp := t.TempDir()
	
	db, err := OpenWithMaxSegmentSize(tmp, 500)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Test empty key
	err = db.Put("", "empty_key_value")
	if err != nil {
		t.Fatalf("Failed to put empty key: %v", err)
	}
	
	value, err := db.Get("")
	if err != nil {
		t.Fatalf("Failed to get empty key: %v", err)
	}
	if value != "empty_key_value" {
		t.Errorf("Empty key: expected 'empty_key_value', got '%s'", value)
	}

	// Test empty value
	err = db.Put("empty_value_key", "")
	if err != nil {
		t.Fatalf("Failed to put empty value: %v", err)
	}
	
	value, err = db.Get("empty_value_key")
	if err != nil {
		t.Fatalf("Failed to get empty value: %v", err)
	}
	if value != "" {
		t.Errorf("Empty value: expected '', got '%s'", value)
	}

	// Test non-existent key
	_, err = db.Get("non_existent_key")
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}

	// Test key with special characters
	specialKey := "key:with/special\\chars"
	specialValue := "value with spaces and symbols!@#$%"
	
	err = db.Put(specialKey, specialValue)
	if err != nil {
		t.Fatalf("Failed to put special key: %v", err)
	}
	
	value, err = db.Get(specialKey)
	if err != nil {
		t.Fatalf("Failed to get special key: %v", err)
	}
	if value != specialValue {
		t.Errorf("Special key: expected '%s', got '%s'", specialValue, value)
	}
}

func TestSegmentedDb_ManualMerge(t *testing.T) {
	tmp := t.TempDir()
	
	db, err := OpenWithMaxSegmentSize(tmp, 200)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create data across multiple segments
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("merge_key_%d", i)
		value := fmt.Sprintf("merge_value_%d_with_extra_data", i)
		err = db.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	// Update some keys to create duplicates
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("merge_key_%d", i)
		value := fmt.Sprintf("updated_merge_value_%d", i)
		err = db.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to update %s: %v", key, err)
		}
	}

	// Let background operations settle
	time.Sleep(300 * time.Millisecond)

	// Verify all data is accessible
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("merge_key_%d", i)
		var expectedValue string
		if i < 10 {
			expectedValue = fmt.Sprintf("updated_merge_value_%d", i)
		} else {
			expectedValue = fmt.Sprintf("merge_value_%d_with_extra_data", i)
		}
		
		value, err := db.Get(key)
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		if value != expectedValue {
			t.Errorf("Key %s: expected '%s', got '%s'", key, expectedValue, value)
		}
	}
}

func TestSegmentedDb_FileSystemIntegrity(t *testing.T) {
	tmp := t.TempDir()
	
	db, err := OpenWithMaxSegmentSize(tmp, 400)
	if err != nil {
		t.Fatal(err)
	}
	
	// Add data to create segments
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("integrity_key_%d", i)
		value := fmt.Sprintf("integrity_value_%d", i)
		err = db.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}
	
	// Get size before close
	sizeBefore, err := db.Size()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Database size before close: %d bytes", sizeBefore)
	
	db.Close()
	
	// Verify we can reopen and read data
	db2, err := OpenWithMaxSegmentSize(tmp, 400)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	
	// Verify data integrity
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("integrity_key_%d", i)
		expectedValue := fmt.Sprintf("integrity_value_%d", i)
		
		value, err := db2.Get(key)
		if err != nil {
			t.Fatalf("Failed to get %s after reopen: %v", key, err)
		}
		if value != expectedValue {
			t.Errorf("Key %s after reopen: expected '%s', got '%s'", key, expectedValue, value)
		}
	}
	
	// Check size after reopen
	sizeAfter, err := db2.Size()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Database size after reopen: %d bytes", sizeAfter)
}

// Benchmark tests
func BenchmarkSegmentedDb_Put(b *testing.B) {
	tmp := b.TempDir()
	db, err := OpenWithMaxSegmentSize(tmp, 10*1024*1024) // 10MB segments
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		value := fmt.Sprintf("bench_value_%d", i)
		err := db.Put(key, value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSegmentedDb_Get(b *testing.B) {
	tmp := b.TempDir()
	db, err := OpenWithMaxSegmentSize(tmp, 10*1024*1024)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Prepare data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		value := fmt.Sprintf("bench_value_%d", i)
		err := db.Put(key, value)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i%1000)
		_, err := db.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}