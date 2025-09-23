package client

import (
	"errors"
	"fmt"
	"net"
	"syscall"

	"github.com/distribuidos-Coffee-Shop-Analysis/client/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/client/protocol"
	"github.com/op/go-logging"
)

// Writer handles sending batches of bets to the server
type Writer struct {
	conn     net.Conn
	agency   int
	clientID string
	log      *logging.Logger
}

// NewWriter creates a new Writer instance
func NewWriter(conn net.Conn, agency int, clientID string) *Writer {
	return &Writer{
		conn:     conn,
		agency:   agency,
		clientID: clientID,
		log:      log,
	}
}

// SendBatch sends a batch of bets to the server using an existing connection
func (w *Writer) SendBatch(bets []protocol.BetMessage, eof bool) error {
	batchMessage := protocol.NewBatchMessage(w.agency, bets, eof)

	for i := 0; i < common.MAX_SEND_RETRIES; i++ {
		// Try to send the message
		err := protocol.SendMessage(w.conn, batchMessage)
		if err == nil {
			return nil // Batch sent successfully
		}

		if errors.Is(err, syscall.EPIPE) {
			log.Infof("Connection closed by server (broken pipe)")
			return err
		}

		w.log.Errorf("action: send_batch | result: fail | attempt: %d | client_id: %v | error: %v",
			i+1, w.clientID, err)
	}

	return fmt.Errorf("failed to send batch after %d attempts", common.MAX_SEND_RETRIES)
}

// CalculateMessageSize estimates the custom protocol size of a batch message
func (w *Writer) CalculateMessageSize(bets []protocol.BetMessage) int {
	batch := protocol.NewBatchMessage(w.agency, bets, false) 
	data, err := protocol.SerializeMessage(batch)
	if err != nil {
		return 0
	}
	return len(data) + common.LENGTH_PREFIX_BYTES // +4 for length prefix
}
