package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"syscall"

	"github.com/distribuidos-Coffee-Shop-Analysis/client/common"
)

// SerializeMessage converts a message to custom protocol format
func SerializeMessage(message interface{}) ([]byte, error) {
	var data []byte

	switch msg := message.(type) {

	case *BatchMessage:
		data = append(data, byte(MessageTypeBatch))
		data = append(data, byte(msg.DatasetType))
		content := fmt.Sprintf("%d|%d|%d", msg.BatchIndex, boolToInt(msg.EOF), len(msg.Records))
		for _, record := range msg.Records {
			content += "|" + record.Serialize()
		}
		data = append(data, []byte(content)...)

	default:
		return nil, fmt.Errorf("unsupported message type")
	}

	return data, nil
}

// DeserializeMessage parses custom protocol data into a message
func DeserializeMessage(data []byte) (interface{}, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("message too short")
	}

	messageType := MessageType(data[0])

	if messageType != MessageTypeBatch {
		return nil, fmt.Errorf("unsupported message type: %d", messageType)
	}

	datasetType := DatasetType(data[1])
	content := string(data[2:])

	return parseResponseMessage(datasetType, content)
}

// parseResponseMessage parses response message from pipe-delimited content
func parseResponseMessage(datasetType DatasetType, content string) (*BatchMessage, error) {
	if len(content) < 1 {
		return nil, fmt.Errorf("invalid batch message: too short")
	}

	contentParts := strings.Split(content, "|")

	if len(contentParts) < 3 {
		return nil, fmt.Errorf("invalid batch message format: missing BatchIndex, EOF or RecordCount")
	}

	batchIndex := 0
	if len(contentParts) > 0 {
		if val := contentParts[0]; val != "" {
			fmt.Sscanf(val, "%d", &batchIndex)
		}
	}

	eof := false
	if len(contentParts) > 1 {
		eof = contentParts[1] == "1"
	}

	recordCount := 0
	if len(contentParts) > 2 {
		fmt.Sscanf(contentParts[2], "%d", &recordCount)
	}

	// Parse records based on dataset type
	records := make([]Record, 0, recordCount)

	// Q2 uses special dual format, others use standard format
	var dataParts []string
	if datasetType == DatasetQ2 {
		dataParts = contentParts[2:] // Skip only BatchIndex and EOF for Q2
	} else {
		dataParts = contentParts[3:] // Skip BatchIndex, EOF and RecordCount for others
	}

	// Determine record type and fields per record based on dataset type
	switch datasetType {
	case DatasetQ1:
		// Q1: transaction_id, final_amount
		for i := 0; i < recordCount; i++ {
			startIdx := i * Q1RecordParts
			endIdx := startIdx + Q1RecordParts
			if endIdx <= len(dataParts) {
				recordFields := dataParts[startIdx:endIdx]
				record, err := NewQ1RecordFromParts(recordFields)
				if err != nil {
					fmt.Printf("Error parsing Q1 record %d: %v\n", i, err)
					continue
				}
				records = append(records, record)
			}
		}
	case DatasetQ2:
		// Q2 dual format: BatchIndex|EOF|Count1|Records1|Count2|Records2
		fmt.Printf("action: parse_q2_dual | content_parts: %d | data_parts: %d\n", len(contentParts), len(dataParts))

		if len(dataParts) < 2 {
			return nil, fmt.Errorf("invalid Q2 dual format: insufficient data parts")
		}

		// Parse count of best selling records
		count1, err := strconv.Atoi(dataParts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid Q2 best selling count: %v", err)
		}
		fmt.Printf("action: parse_q2_best_selling | count: %d\n", count1)

		currentIdx := 1

		// Parse best selling records
		for i := 0; i < count1; i++ {
			endIdx := currentIdx + Q2BestSellingWithNameRecordParts
			if endIdx > len(dataParts) {
				return nil, fmt.Errorf("insufficient data for Q2 best selling record %d", i)
			}
			recordFields := dataParts[currentIdx:endIdx]
			record, err := NewQ2BestSellingWithNameRecordFromParts(recordFields)
			if err != nil {
				fmt.Printf("action: parse_q2_best_selling | index: %d | error: %v\n", i, err)
				currentIdx = endIdx
				continue
			}
			fmt.Printf("action: parse_q2_best_selling | index: %d | year_month: %s | item_name: %s | qty: %s\n",
				i, record.YearMonth, record.ItemName, record.SellingsQty)
			records = append(records, record)
			currentIdx = endIdx
		}

		// Parse count of most profits records
		if currentIdx >= len(dataParts) {
			return nil, fmt.Errorf("missing Q2 most profits count")
		}
		count2, err := strconv.Atoi(dataParts[currentIdx])
		if err != nil {
			return nil, fmt.Errorf("invalid Q2 most profits count: %v", err)
		}
		fmt.Printf("action: parse_q2_most_profits | count: %d\n", count2)
		currentIdx++

		// Parse most profits records
		for i := 0; i < count2; i++ {
			endIdx := currentIdx + Q2MostProfitsWithNameRecordParts
			if endIdx > len(dataParts) {
				return nil, fmt.Errorf("insufficient data for Q2 most profits record %d", i)
			}
			recordFields := dataParts[currentIdx:endIdx]
			record, err := NewQ2MostProfitsWithNameRecordFromParts(recordFields)
			if err != nil {
				fmt.Printf("action: parse_q2_most_profits | index: %d | error: %v\n", i, err)
				currentIdx = endIdx
				continue
			}
			fmt.Printf("action: parse_q2_most_profits | index: %d | year_month: %s | item_name: %s | profit: %s\n",
				i, record.YearMonth, record.ItemName, record.ProfitSum)
			records = append(records, record)
			currentIdx = endIdx
		}
	case DatasetQ3:
		// Q3: year_half_created_at, store_name, tpv
		for i := 0; i < recordCount; i++ {
			startIdx := i * Q3JoinedRecordParts
			endIdx := startIdx + Q3JoinedRecordParts
			if endIdx <= len(dataParts) {
				recordFields := dataParts[startIdx:endIdx]
				record, err := NewQ3JoinedRecordFromParts(recordFields)
				if err != nil {
					fmt.Printf("Error parsing Q3 record %d: %v\n", i, err)
					continue
				}
				records = append(records, record)
			}
		}
	case DatasetQ4:
		// Q4: store_name, birthdate
		for i := 0; i < recordCount; i++ {
			startIdx := i * Q4JoinedWithStoreAndUserRecordParts
			endIdx := startIdx + Q4JoinedWithStoreAndUserRecordParts
			if endIdx <= len(dataParts) {
				recordFields := dataParts[startIdx:endIdx]
				record, err := NewQ4JoinedWithStoreAndUserRecordFromParts(recordFields)
				if err != nil {
					fmt.Printf("Error parsing Q4 record %d: %v\n", i, err)
					continue
				}
				records = append(records, record)
			}
		}
	default:
		return nil, fmt.Errorf("unknown dataset type for queries: %d", datasetType)
	}

	return &BatchMessage{
		Type:        MessageTypeBatch,
		DatasetType: datasetType,
		BatchIndex:  batchIndex,
		Records:     records,
		EOF:         eof,
	}, nil
}

// SendMessage serializes message and sends with length prefix
func SendMessage(conn net.Conn, message interface{}) error {
	data, err := SerializeMessage(message)
	if err != nil {
		return fmt.Errorf("error serializing message: %w", err)
	}

	// Send length prefix
	length := uint32(len(data))
	lengthBytes := make([]byte, common.LENGTH_PREFIX_BYTES)
	binary.BigEndian.PutUint32(lengthBytes, length)

	if err := writeExact(conn, lengthBytes); err != nil {
		if errors.Is(err, syscall.EPIPE) {
			return fmt.Errorf("error sending length: broken pipe")
		}
		return fmt.Errorf("error sending length: %w", err)
	}

	// Send message data
	if err := writeExact(conn, data); err != nil {
		if errors.Is(err, syscall.EPIPE) {
			return fmt.Errorf("error sending data: broken pipe")
		}
		return fmt.Errorf("error sending data: %w", err)
	}

	return nil
}

// RecvMessage reads length-prefixed message and deserializes
func RecvMessage(conn net.Conn, v interface{}) error {
	// Read length prefix (4 bytes)
	lengthBytes := make([]byte, 4)
	if _, err := readExact(conn, lengthBytes); err != nil {
		if err == io.EOF {
			return io.EOF
		}
		return fmt.Errorf("error reading length: %w", err)
	}

	length := binary.BigEndian.Uint32(lengthBytes)

	// Read message data
	data := make([]byte, length)
	if _, err := readExact(conn, data); err != nil {
		if err == io.EOF {
			return io.EOF // Unexpected EOF, incomplete message
		}
		return fmt.Errorf("error reading data: %w", err)
	}

	// Deserialize message
	message, err := DeserializeMessage(data)
	if err != nil {
		return fmt.Errorf("error deserializing message: %w", err)
	}

	// Copy to destination
	switch dst := v.(type) {
	case *BatchMessage:
		if src, ok := message.(*BatchMessage); ok {
			*dst = *src
		} else {
			return fmt.Errorf("type mismatch: expected BatchMessage")
		}
	default:
		return fmt.Errorf("unsupported destination type")
	}

	return nil
}

// readExact reads exactly len(buf) bytes from conn
func readExact(conn net.Conn, buf []byte) (int, error) {
	totalRead := 0
	for totalRead < len(buf) {
		n, err := conn.Read(buf[totalRead:])
		if err != nil {
			return totalRead, err
		}
		totalRead += n
	}
	return totalRead, nil
}

// writeExact writes exactly all bytes in data over conn
func writeExact(conn net.Conn, data []byte) error {
	totalWritten := 0
	for totalWritten < len(data) {
		n, err := conn.Write(data[totalWritten:])
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("connection closed during write")
		}
		totalWritten += n
	}
	return nil
}
