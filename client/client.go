package client

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/distribuidos-Coffee-Shop-Analysis/client/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/client/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             string
	ServerAddress  string
	LoopAmount     int
	LoopPeriod     time.Duration
	DatasetPaths   map[protocol.DatasetType]string
	BatchMaxAmount int
	OutputDir      string
}

type Client struct {
	config       ClientConfig
	conn         net.Conn
	shutdownChan chan struct{}
	writer       *Writer
	listener     *Listener
}

// NewClient Initializes a new client receiving the configuration
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config:       config,
		shutdownChan: make(chan struct{}),
	}

	// Setup signal handler for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Goroutine to handle shutdown signal
	go func() {
		<-sigChan
		log.Infof("action: shutdown | result: in_progress | client_id: %v | msg: received SIGTERM", client.config.ID)

		// Closing the channel notifies the receivers
		// we have to shutdown the program
		close(client.shutdownChan)

		log.Infof("action: shutdown | result: signal_sent | client_id: %v", client.config.ID)
	}()

	// Initialize client components
	if err := client.initClient(); err != nil {
		log.Errorf("action: init_client | result: fail | client_id: %v | error: %v", client.config.ID, err)
		return nil
	}

	return client
}

// initClient initializes all client components
func (c *Client) initClient() error {
	// Create client socket
	if err := c.createClientSocket(); err != nil {
		return err
	}

	c.writer = NewWriter(c.conn, c.config.ID)
	c.listener = NewListener(c.conn, c.config.ID, c.config.OutputDir)

	return nil
}

// CreateClientSocket Initializes client socket
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf("action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}
	c.conn = conn
	return nil
}

// Main function:
// It processes the CSV files in streaming mode. Just store one batch in memory at a time
// Once the CSVs are fully processed, listen from the server for the query responses.
// Finally, we close the connection and the CSV file

func (c *Client) StartClientWithDatasets() {
	defer c.conn.Close() // Close the connection

	for datasetType, csvPath := range c.config.DatasetPaths {
		select {
		case <-c.shutdownChan:
			log.Infof("action: shutdown_received | result: exiting_datasets | client_id: %v", c.config.ID)
			return
		default:
		}

		log.Infof("action: process_dataset | result: start | client_id: %v | dataset_type: %d | file: %s",
			c.config.ID, datasetType, csvPath)

		if err := c.processDataset(datasetType, csvPath); err != nil {
			log.Errorf("action: process_dataset | result: fail | client_id: %v | dataset_type: %d | error: %v",
				c.config.ID, datasetType, err)
			return
		}

		log.Infof("action: process_dataset | result: success | client_id: %v | dataset_type: %d",
			c.config.ID, datasetType)
	}

	log.Infof("action: process_all_datasets | result: success | client_id: %v", c.config.ID)

	// After sending all datasets, start listening for query responses
	log.Infof("action: start_listening | result: start | client_id: %v | msg: waiting for query responses", c.config.ID)
	if err := c.listener.ReceiveQueryResponses(c.shutdownChan); err != nil {
		log.Errorf("action: receive_queries | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	log.Infof("action: start_listening | result: success | client_id: %v | msg: all query responses received", c.config.ID)
}

func (c *Client) processDataset(datasetType protocol.DatasetType, csvPath string) error {
	fileInfo, err := os.Stat(csvPath)
	if err != nil {
		return fmt.Errorf("failed to access path %s: %w", csvPath, err)
	}

	if fileInfo.IsDir() {
		return c.processDirectory(datasetType, csvPath)
	}

	fileManager, err := c.createFileManager(datasetType, csvPath)
	if err != nil {
		return fmt.Errorf("failed to create file manager: %w", err)
	}
	defer fileManager.Close()

	return c.processRecordsFromFile(datasetType, fileManager, true)
}

// processDirectory processes all CSV files in a directory for the given dataset type
func (c *Client) processDirectory(datasetType protocol.DatasetType, dirPath string) error {

	files, err := filepath.Glob(filepath.Join(dirPath, "*.csv"))
	if err != nil {
		return fmt.Errorf("failed to list CSV files in directory %s: %w", dirPath, err)
	}

	if len(files) == 0 {
		return fmt.Errorf("no CSV files found in directory %s", dirPath)
	}

	log.Infof("action: process_directory | result: start | client_id: %v | dataset_type: %d | directory: %s | files_count: %d",
		c.config.ID, datasetType, dirPath, len(files))

	for i, filePath := range files {
		select {
		case <-c.shutdownChan:
			log.Infof("action: shutdown_received | result: exiting_directory | client_id: %v", c.config.ID)
			return nil
		default:
		}

		isLastFile := (i == len(files)-1)
		log.Infof("action: process_file | result: start | client_id: %v | dataset_type: %d | file: %s | is_last: %v",
			c.config.ID, datasetType, filePath, isLastFile)

		fileManager, err := c.createFileManager(datasetType, filePath)
		if err != nil {
			return fmt.Errorf("failed to create file manager for %s: %w", filePath, err)
		}

		err = c.processRecordsFromFile(datasetType, fileManager, isLastFile)
		fileManager.Close()

		if err != nil {
			return fmt.Errorf("failed to process file %s: %w", filePath, err)
		}

		log.Infof("action: process_file | result: success | client_id: %v | dataset_type: %d | file: %s",
			c.config.ID, datasetType, filePath)
	}

	log.Infof("action: process_directory | result: success | client_id: %v | dataset_type: %d | directory: %s",
		c.config.ID, datasetType, dirPath)

	return nil
}

func (c *Client) createFileManager(datasetType protocol.DatasetType, csvPath string) (*FileManager, error) {
	switch datasetType {
	case protocol.DatasetMenuItems:
		return NewMenuItemFileManager(csvPath)
	case protocol.DatasetStores:
		return NewStoreFileManager(csvPath)
	case protocol.DatasetTransactionItems:
		return NewTransactionItemFileManager(csvPath)
	case protocol.DatasetTransactions:
		return NewTransactionFileManager(csvPath)
	case protocol.DatasetUsers:
		return NewUserFileManager(csvPath)
	default:
		return nil, fmt.Errorf("unsupported dataset type: %d", datasetType)
	}
}

func (c *Client) processRecordsFromFile(datasetType protocol.DatasetType, fileManager *FileManager, isLastFile bool) error {
	var currentBatch []protocol.Record
	validCount := 0

	for {
		select {
		case <-c.shutdownChan:
			log.Infof("action: shutdown_received | result: exiting_loop | client_id: %v", c.config.ID)
			return nil
		default:
		}

		// Check if we've reached the maximum number of valid records
		if validCount >= common.MAX_CSV_RECORDS {
			log.Infof("action: read_csv_streaming | result: limit_reached | client_id: %v | max_records: %d | stopping early",
				c.config.ID, common.MAX_CSV_RECORDS)
			break
		}

		// Get next valid record
		record, isEOF, err := fileManager.NextRecord()
		if err != nil {
			log.Errorf("action: read_csv_streaming | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return err
		}
		if isEOF {
			break
		}

		// Check if adding this record would exceed size or count limits
		testBatch := append(currentBatch, record)
		batchSize := c.calculateBatchSize(datasetType, testBatch)

		if len(testBatch) > c.config.BatchMaxAmount || batchSize > common.MAX_BATCH_SIZE_BYTES {
			if err := c.sendBatch(datasetType, currentBatch, false); err != nil {
				if errors.Is(err, syscall.EPIPE) {
					log.Infof("Connection closed by server (broken pipe)")
					return err
				}
				return err
			}
			// Start new batch with current record
			currentBatch = []protocol.Record{record}
		} else {
			// Add to current batch as it doesn't exceed limits
			currentBatch = testBatch
		}

		validCount++
	}

	// Send final batch if there are remaining records
	if len(currentBatch) > 0 {
		if err := c.sendBatch(datasetType, currentBatch, isLastFile); err != nil {
			log.Errorf("action: send_last_batch | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return err
		}
	}

	log.Infof("action: csv_streaming_finished | result: success | client_id: %v | valid_records: %d | dataset_type: %d",
		c.config.ID, validCount, datasetType)
	return nil
}

func (c *Client) calculateBatchSize(datasetType protocol.DatasetType, records []protocol.Record) int {
	switch datasetType {
	case protocol.DatasetMenuItems:
		menuItems := make([]protocol.MenuItemRecord, len(records))
		for i, record := range records {
			menuItems[i] = record.(protocol.MenuItemRecord)
		}
		return c.writer.CalculateMenuItemBatchSize(menuItems)
	case protocol.DatasetStores:
		stores := make([]protocol.StoreRecord, len(records))
		for i, record := range records {
			stores[i] = record.(protocol.StoreRecord)
		}
		return c.writer.CalculateStoreBatchSize(stores)
	case protocol.DatasetTransactionItems:
		txnItems := make([]protocol.TransactionItemRecord, len(records))
		for i, record := range records {
			txnItems[i] = record.(protocol.TransactionItemRecord)
		}
		return c.writer.CalculateTransactionItemBatchSize(txnItems)
	case protocol.DatasetTransactions:
		transactions := make([]protocol.TransactionRecord, len(records))
		for i, record := range records {
			transactions[i] = record.(protocol.TransactionRecord)
		}
		return c.writer.CalculateTransactionBatchSize(transactions)
	case protocol.DatasetUsers:
		users := make([]protocol.UserRecord, len(records))
		for i, record := range records {
			users[i] = record.(protocol.UserRecord)
		}
		return c.writer.CalculateUserBatchSize(users)
	default:
		return 0
	}
}

func (c *Client) sendBatch(datasetType protocol.DatasetType, records []protocol.Record, eof bool) error {
	switch datasetType {
	case protocol.DatasetMenuItems:
		menuItems := make([]protocol.MenuItemRecord, len(records))
		for i, record := range records {
			menuItems[i] = record.(protocol.MenuItemRecord)
		}
		return c.writer.SendMenuItemBatch(menuItems, eof)
	case protocol.DatasetStores:
		stores := make([]protocol.StoreRecord, len(records))
		for i, record := range records {
			stores[i] = record.(protocol.StoreRecord)
		}
		return c.writer.SendStoreBatch(stores, eof)
	case protocol.DatasetTransactionItems:
		txnItems := make([]protocol.TransactionItemRecord, len(records))
		for i, record := range records {
			txnItems[i] = record.(protocol.TransactionItemRecord)
		}
		return c.writer.SendTransactionItemBatch(txnItems, eof)
	case protocol.DatasetTransactions:
		transactions := make([]protocol.TransactionRecord, len(records))
		for i, record := range records {
			transactions[i] = record.(protocol.TransactionRecord)
		}
		return c.writer.SendTransactionBatch(transactions, eof)
	case protocol.DatasetUsers:
		users := make([]protocol.UserRecord, len(records))
		for i, record := range records {
			users[i] = record.(protocol.UserRecord)
		}
		return c.writer.SendUserBatch(users, eof)
	default:
		return fmt.Errorf("unsupported dataset type: %d", datasetType)
	}
}
