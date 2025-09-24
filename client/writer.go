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

// Writer handles sending batches of records to the server
type Writer struct {
	conn     net.Conn
	clientID string
	log      *logging.Logger
}

// NewWriter creates a new Writer instance
func NewWriter(conn net.Conn, clientID string) *Writer {
	return &Writer{
		conn:     conn,
		clientID: clientID,
		log:      log,
	}
}

func (w *Writer) SendMenuItemBatch(records []protocol.MenuItemRecord, eof bool) error {
	recordInterfaces := make([]protocol.Record, len(records))
	for i, record := range records {
		recordInterfaces[i] = record
	}

	batchMessage := protocol.NewBatchMessage(protocol.DatasetMenuItems, recordInterfaces, eof)
	return w.sendBatch(batchMessage)
}

func (w *Writer) SendStoreBatch(records []protocol.StoreRecord, eof bool) error {
	recordInterfaces := make([]protocol.Record, len(records))
	for i, record := range records {
		recordInterfaces[i] = record
	}

	batchMessage := protocol.NewBatchMessage(protocol.DatasetStores, recordInterfaces, eof)
	return w.sendBatch(batchMessage)
}

func (w *Writer) SendTransactionItemBatch(records []protocol.TransactionItemRecord, eof bool) error {
	recordInterfaces := make([]protocol.Record, len(records))
	for i, record := range records {
		recordInterfaces[i] = record
	}

	batchMessage := protocol.NewBatchMessage(protocol.DatasetTransactionItems, recordInterfaces, eof)
	return w.sendBatch(batchMessage)
}

func (w *Writer) SendTransactionBatch(records []protocol.TransactionRecord, eof bool) error {
	recordInterfaces := make([]protocol.Record, len(records))
	for i, record := range records {
		recordInterfaces[i] = record
	}

	batchMessage := protocol.NewBatchMessage(protocol.DatasetTransactions, recordInterfaces, eof)
	return w.sendBatch(batchMessage)
}

func (w *Writer) SendUserBatch(records []protocol.UserRecord, eof bool) error {
	recordInterfaces := make([]protocol.Record, len(records))
	for i, record := range records {
		recordInterfaces[i] = record
	}

	batchMessage := protocol.NewBatchMessage(protocol.DatasetUsers, recordInterfaces, eof)
	return w.sendBatch(batchMessage)
}

func (w *Writer) sendBatch(batchMessage *protocol.BatchMessage) error {
	for i := 0; i < common.MAX_SEND_RETRIES; i++ {
		err := protocol.SendMessage(w.conn, batchMessage)
		if err == nil {
			return nil 
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

func (w *Writer) CalculateMenuItemBatchSize(records []protocol.MenuItemRecord) int {
	recordInterfaces := make([]protocol.Record, len(records))
	for i, record := range records {
		recordInterfaces[i] = record
	}

	batch := protocol.NewBatchMessage(protocol.DatasetMenuItems, recordInterfaces, false)
	data, err := protocol.SerializeMessage(batch)
	if err != nil {
		return 0
	}
	return len(data) + common.LENGTH_PREFIX_BYTES
}

func (w *Writer) CalculateStoreBatchSize(records []protocol.StoreRecord) int {
	recordInterfaces := make([]protocol.Record, len(records))
	for i, record := range records {
		recordInterfaces[i] = record
	}

	batch := protocol.NewBatchMessage(protocol.DatasetStores, recordInterfaces, false)
	data, err := protocol.SerializeMessage(batch)
	if err != nil {
		return 0
	}
	return len(data) + common.LENGTH_PREFIX_BYTES
}

func (w *Writer) CalculateTransactionItemBatchSize(records []protocol.TransactionItemRecord) int {
	recordInterfaces := make([]protocol.Record, len(records))
	for i, record := range records {
		recordInterfaces[i] = record
	}

	batch := protocol.NewBatchMessage(protocol.DatasetTransactionItems, recordInterfaces, false)
	data, err := protocol.SerializeMessage(batch)
	if err != nil {
		return 0
	}
	return len(data) + common.LENGTH_PREFIX_BYTES
}

func (w *Writer) CalculateTransactionBatchSize(records []protocol.TransactionRecord) int {
	recordInterfaces := make([]protocol.Record, len(records))
	for i, record := range records {
		recordInterfaces[i] = record
	}

	batch := protocol.NewBatchMessage(protocol.DatasetTransactions, recordInterfaces, false)
	data, err := protocol.SerializeMessage(batch)
	if err != nil {
		return 0
	}
	return len(data) + common.LENGTH_PREFIX_BYTES
}

func (w *Writer) CalculateUserBatchSize(records []protocol.UserRecord) int {
	recordInterfaces := make([]protocol.Record, len(records))
	for i, record := range records {
		recordInterfaces[i] = record
	}

	batch := protocol.NewBatchMessage(protocol.DatasetUsers, recordInterfaces, false)
	data, err := protocol.SerializeMessage(batch)
	if err != nil {
		return 0
	}
	return len(data) + common.LENGTH_PREFIX_BYTES
}
