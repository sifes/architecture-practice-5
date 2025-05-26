package datastore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type entry struct {
	key         string
	valueType   uint8
	stringValue string
	int64Value  int64
}

// New format:
// 0           4    8     kl+8  kl+9     kl+10 ... <-- offset
// (full size) (kl) (key) (type) (value_data)       <-- content
// 4           4    ....  1      depends on type    <-- length

func (e *entry) Encode() []byte {
	kl := len(e.key)
	var valueData []byte

	// Encode value based on type
	switch e.valueType {
	case TypeString:
		valueData = make([]byte, 4+len(e.stringValue))
		binary.LittleEndian.PutUint32(valueData, uint32(len(e.stringValue)))
		copy(valueData[4:], e.stringValue)
	case TypeInt64:
		valueData = make([]byte, 8)
		binary.LittleEndian.PutUint64(valueData, uint64(e.int64Value))
	default:
		// For backward compatibility, treat unknown types as strings
		valueData = make([]byte, 4+len(e.stringValue))
		binary.LittleEndian.PutUint32(valueData, uint32(len(e.stringValue)))
		copy(valueData[4:], e.stringValue)
		e.valueType = TypeString
	}

	// Total size: header(4) + key_len(4) + key + type(1) + value_data
	size := 4 + 4 + kl + 1 + len(valueData)
	result := make([]byte, size)

	// Write header
	binary.LittleEndian.PutUint32(result, uint32(size))
	binary.LittleEndian.PutUint32(result[4:], uint32(kl))

	// Write key
	copy(result[8:], e.key)

	// Write type
	result[8+kl] = e.valueType

	// Write value data
	copy(result[8+kl+1:], valueData)

	return result
}

func (e *entry) Decode(input []byte) error {
	if len(input) < 9 { // minimum: size(4) + key_len(4) + type(1)
		return fmt.Errorf("input too short")
	}

	// Read key length and key
	keyLen := binary.LittleEndian.Uint32(input[4:8])
	if len(input) < int(8+keyLen+1) {
		return fmt.Errorf("input too short for key")
	}

	e.key = string(input[8 : 8+keyLen])

	// Read type
	typeOffset := 8 + keyLen
	if typeOffset >= uint32(len(input)) {
		// Backward compatibility: if no type byte, assume string
		e.valueType = TypeString
		e.stringValue = string(input[8+keyLen:])
		return nil
	}

	e.valueType = input[typeOffset]
	valueDataStart := typeOffset + 1

	if int(valueDataStart) >= len(input) {
		return fmt.Errorf("no value data")
	}

	valueData := input[valueDataStart:]

	// Decode value based on type
	switch e.valueType {
	case TypeString:
		if len(valueData) < 4 {
			return fmt.Errorf("invalid string value data")
		}
		strLen := binary.LittleEndian.Uint32(valueData[:4])
		if len(valueData) < int(4+strLen) {
			return fmt.Errorf("string value data too short")
		}
		e.stringValue = string(valueData[4 : 4+strLen])

	case TypeInt64:
		if len(valueData) < 8 {
			return fmt.Errorf("invalid int64 value data")
		}
		e.int64Value = int64(binary.LittleEndian.Uint64(valueData[:8]))

	default:
		// Backward compatibility: treat unknown types as strings
		// Try to decode as old format (length + string)
		if len(valueData) >= 4 {
			strLen := binary.LittleEndian.Uint32(valueData[:4])
			if len(valueData) >= int(4+strLen) {
				e.stringValue = string(valueData[4 : 4+strLen])
				e.valueType = TypeString
			} else {
				// Fallback: treat entire value data as string
				e.stringValue = string(valueData)
				e.valueType = TypeString
			}
		} else {
			e.stringValue = string(valueData)
			e.valueType = TypeString
		}
	}

	return nil
}

func (e *entry) DecodeFromReader(in *bufio.Reader) (int, error) {
	// Read size header
	sizeBuf, err := in.Peek(4)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, err
		}
		return 0, fmt.Errorf("DecodeFromReader, cannot read size: %w", err)
	}

	totalSize := int(binary.LittleEndian.Uint32(sizeBuf))
	if totalSize < 9 { // minimum size
		return 0, fmt.Errorf("invalid entry size: %d", totalSize)
	}

	// Read entire entry
	buf := make([]byte, totalSize)
	n, err := in.Read(buf)
	if err != nil {
		return n, fmt.Errorf("DecodeFromReader, cannot read entry: %w", err)
	}

	if n != totalSize {
		return n, fmt.Errorf("DecodeFromReader, incomplete read: expected %d, got %d", totalSize, n)
	}

	err = e.Decode(buf)
	return n, err
}
