package datastore

import (
	"bufio"
	"bytes"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	e := entry{
		key:         "key",
		valueType:   TypeString,
		stringValue: "value",
	}
	e.Decode(e.Encode())
	if e.key != "key" {
		t.Error("incorrect key")
	}
	if e.stringValue != "value" {
		t.Error("incorrect value")
	}
	if e.valueType != TypeString {
		t.Error("incorrect type")
	}
}

func TestReadValue(t *testing.T) {
	var (
		a, b entry
	)
	a = entry{
		key:         "key",
		valueType:   TypeString,
		stringValue: "test-value",
	}
	originalBytes := a.Encode()

	b.Decode(originalBytes)
	t.Log("encode/decode", a, b)
	if a.key != b.key || a.valueType != b.valueType || a.stringValue != b.stringValue {
		t.Error("Encode/Decode mismatch")
	}

	b = entry{}
	n, err := b.DecodeFromReader(bufio.NewReader(bytes.NewReader(originalBytes)))
	if err != nil {
		t.Fatal(err)
	}
	t.Log("encode/decodeFromReader", a, b)
	if a.key != b.key || a.valueType != b.valueType || a.stringValue != b.stringValue {
		t.Error("Encode/DecodeFromReader mismatch")
	}
	if n != len(originalBytes) {
		t.Errorf("DecodeFromReader() read %d bytes, expected %d", n, len(originalBytes))
	}
}
