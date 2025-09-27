package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
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
		content := fmt.Sprintf("%d|%d", boolToInt(msg.EOF), len(msg.Records))
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
	if len(data) < 1 {
		return nil, fmt.Errorf("empty message")
	}

	content := string(data[1:])
	return parseResponseMessage(content)
}

// parseResponseMessage parses response message from pipe-delimited content
func parseResponseMessage(content string) (*ResponseMessage, error) {
	parts := strings.Split(content, "|")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid response format")
	}

	success := parts[0] == "1"
	errorMsg := parts[1]

	return &ResponseMessage{
		Type:    MessageTypeResponse,
		Success: success,
		Error:   errorMsg,
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
	case *ResponseMessage:
		if src, ok := message.(*ResponseMessage); ok {
			*dst = *src
		} else {
			return fmt.Errorf("type mismatch: expected ResponseMessage")
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
