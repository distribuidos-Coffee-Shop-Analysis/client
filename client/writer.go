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
	conn         net.Conn
	clientID     string
	log          *logging.Logger
	batchIndexes map[protocol.DatasetType]int
}

// NewWriter creates a new Writer instance
func NewWriter(conn net.Conn, clientID string) *Writer {
	return &Writer{
		conn:         conn,
		clientID:     clientID,
		log:          log,
		batchIndexes: make(map[protocol.DatasetType]int),
	}
}

// ResetBatchIndex resets the batch index counter for a specific dataset
func (w *Writer) ResetBatchIndex(datasetType protocol.DatasetType) {
	w.batchIndexes[datasetType] = 1
}

func (w *Writer) SendMenuItemBatch(records []protocol.MenuItemRecord, eof bool) error {
	recordInterfaces := make([]protocol.Record, len(records))
	for i, record := range records {
		recordInterfaces[i] = record
	}

	// Increment batch index before creating the message
	w.batchIndexes[protocol.DatasetMenuItems]++
	batchMessage := protocol.NewBatchMessage(protocol.DatasetMenuItems, w.batchIndexes[protocol.DatasetMenuItems], recordInterfaces, eof)
	err := w.sendBatch(batchMessage)
	if err != nil {
		// Decrement on error so retry uses same index
		w.batchIndexes[protocol.DatasetMenuItems]--
	}
	return err
}

func (w *Writer) SendStoreBatch(records []protocol.StoreRecord, eof bool) error {
	recordInterfaces := make([]protocol.Record, len(records))
	for i, record := range records {
		recordInterfaces[i] = record
	}

	// Increment batch index before creating the message
	w.batchIndexes[protocol.DatasetStores]++
	batchMessage := protocol.NewBatchMessage(protocol.DatasetStores, w.batchIndexes[protocol.DatasetStores], recordInterfaces, eof)
	err := w.sendBatch(batchMessage)
	if err != nil {
		// Decrement on error so retry uses same index
		w.batchIndexes[protocol.DatasetStores]--
	}
	return err
}

func (w *Writer) SendTransactionItemBatch(records []protocol.TransactionItemRecord, eof bool) error {
	recordInterfaces := make([]protocol.Record, len(records))
	for i, record := range records {
		recordInterfaces[i] = record
	}

	// Increment batch index before creating the message
	w.batchIndexes[protocol.DatasetTransactionItems]++
	batchMessage := protocol.NewBatchMessage(protocol.DatasetTransactionItems, w.batchIndexes[protocol.DatasetTransactionItems], recordInterfaces, eof)
	err := w.sendBatch(batchMessage)
	if err != nil {
		// Decrement on error so retry uses same index
		w.batchIndexes[protocol.DatasetTransactionItems]--
	}
	return err
}

func (w *Writer) SendTransactionBatch(records []protocol.TransactionRecord, eof bool) error {
	recordInterfaces := make([]protocol.Record, len(records))
	for i, record := range records {
		recordInterfaces[i] = record
	}

	// Increment batch index before creating the message
	w.batchIndexes[protocol.DatasetTransactions]++
	batchMessage := protocol.NewBatchMessage(protocol.DatasetTransactions, w.batchIndexes[protocol.DatasetTransactions], recordInterfaces, eof)
	err := w.sendBatch(batchMessage)
	if err != nil {
		// Decrement on error so retry uses same index
		w.batchIndexes[protocol.DatasetTransactions]--
	}
	return err
}

func (w *Writer) SendUserBatch(records []protocol.UserRecord, eof bool) error {
	recordInterfaces := make([]protocol.Record, len(records))
	for i, record := range records {
		recordInterfaces[i] = record
	}

	// Increment batch index before creating the message
	w.batchIndexes[protocol.DatasetUsers]++
	batchMessage := protocol.NewBatchMessage(protocol.DatasetUsers, w.batchIndexes[protocol.DatasetUsers], recordInterfaces, eof)
	err := w.sendBatch(batchMessage)
	if err != nil {
		// Decrement on error so retry uses same index
		w.batchIndexes[protocol.DatasetUsers]--
	}
	return err
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

	batch := protocol.NewBatchMessage(protocol.DatasetMenuItems, w.batchIndexes[protocol.DatasetMenuItems], recordInterfaces, false)
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

	batch := protocol.NewBatchMessage(protocol.DatasetStores, w.batchIndexes[protocol.DatasetStores], recordInterfaces, false)
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

	batch := protocol.NewBatchMessage(protocol.DatasetTransactionItems, w.batchIndexes[protocol.DatasetTransactionItems], recordInterfaces, false)
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

	batch := protocol.NewBatchMessage(protocol.DatasetTransactions, w.batchIndexes[protocol.DatasetTransactions], recordInterfaces, false)
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

	batch := protocol.NewBatchMessage(protocol.DatasetUsers, w.batchIndexes[protocol.DatasetUsers], recordInterfaces, false)
	data, err := protocol.SerializeMessage(batch)
	if err != nil {
		return 0
	}
	return len(data) + common.LENGTH_PREFIX_BYTES
}
