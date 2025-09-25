package client

import (
	"encoding/csv"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/distribuidos-Coffee-Shop-Analysis/client/protocol"
	"github.com/op/go-logging"
)

// Listener handles receiving query responses from the server and writing to CSV files
type Listener struct {
	conn      net.Conn
	clientID  string
	outputDir string
	log       *logging.Logger
}

const TOTAL_QUERIES_EXPECTED = 4

// NewListener creates a new Listener instance
func NewListener(conn net.Conn, clientID string, outputDir string) *Listener {
	return &Listener{
		conn:      conn,
		clientID:  clientID,
		outputDir: outputDir,
		log:       log,
	}
}

// Waits for and processes query responses from the server
func (l *Listener) ReceiveQueryResponses(shutdownChan <-chan struct{}) error {
	l.log.Infof("action: receive_queries | result: in_progress | client_id: %v | msg: waiting for query responses", l.clientID)

	// Keep track of open CSV writers for each query type
	csvWriters := make(map[protocol.DatasetType]*csv.Writer)
	outputFiles := make(map[protocol.DatasetType]*os.File)

	defer func() {
		for _, writer := range csvWriters {
			writer.Flush()
		}
		for _, file := range outputFiles {
			file.Close()
		}
	}()

	queriesCompleted := make(map[protocol.DatasetType]bool)

	for len(queriesCompleted) < TOTAL_QUERIES_EXPECTED {
		select {
		case <-shutdownChan:
			l.log.Infof("action: receive_queries | result: shutdown | client_id: %v", l.clientID)
			return nil
		default:
		}

		var batchMessage protocol.BatchMessage
		if err := protocol.RecvMessage(l.conn, &batchMessage); err != nil {
			return fmt.Errorf("failed to receive batch message: %w", err)
		}

		datasetType := batchMessage.DatasetType

		if datasetType < protocol.DatasetQ1 || datasetType > protocol.DatasetQ4 {
			return fmt.Errorf("received invalid dataset type for query response: %d", datasetType)
		}

		// Initialize CSV writer if this is the first batch for this query
		if _, exists := csvWriters[datasetType]; !exists {
			file, writer, err := l.createOutputFile(datasetType)
			if err != nil {
				return fmt.Errorf("failed to create output file for query %d: %w", datasetType, err)
			}
			csvWriters[datasetType] = writer
			outputFiles[datasetType] = file

			queryNum := int(datasetType) - int(protocol.DatasetQ1) + 1
			l.log.Infof("action: receive_query | result: in_progress | client_id: %v | query: Q%d", l.clientID, queryNum)
		}

		csvWriter := csvWriters[datasetType]
		for _, record := range batchMessage.Records {
			if err := l.writeRecordToCSV(csvWriter, record); err != nil {
				return fmt.Errorf("failed to write record for query %d: %w", datasetType, err)
			}
		}

		csvWriter.Flush()

		queryNum := int(datasetType) - int(protocol.DatasetQ1) + 1
		l.log.Debugf("action: receive_batch | result: success | client_id: %v | query: Q%d | records_in_batch: %d",
			l.clientID, queryNum, len(batchMessage.Records))

		if batchMessage.EOF {
			queriesCompleted[datasetType] = true
			l.log.Infof("action: receive_query | result: complete | client_id: %v | query: Q%d", l.clientID, queryNum)
		}
	}

	l.log.Infof("action: receive_queries | result: success | client_id: %v | msg: all queries received", l.clientID)
	return nil
}

// Creates a CSV file for the specified dataset type
func (l *Listener) createOutputFile(datasetType protocol.DatasetType) (*os.File, *csv.Writer, error) {
	queryNum := int(datasetType) - int(protocol.DatasetQ1) + 1
	filename := fmt.Sprintf("Q%d_results.csv", queryNum)
	filepath := filepath.Join(l.outputDir, filename)

	if err := os.MkdirAll(l.outputDir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	file, err := os.Create(filepath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create file %s: %w", filepath, err)
	}

	csvWriter := csv.NewWriter(file)

	if err := l.writeCSVHeader(csvWriter, datasetType); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to write CSV header: %w", err)
	}

	return file, csvWriter, nil
}

func (l *Listener) writeCSVHeader(writer *csv.Writer, datasetType protocol.DatasetType) error {
	var header []string

	switch datasetType {
	case protocol.DatasetQ1:
		header = []string{"transaction_id", "final_amount"}
	case protocol.DatasetQ2:
		header = []string{"year_month_created_at", "item_name", "sellings_qty"}
	case protocol.DatasetQ3:
		header = []string{"year_half_created_at", "store_name", "tpv"}
	case protocol.DatasetQ4:
		header = []string{"store_name", "birthdate"}
	default:
		return fmt.Errorf("unknown dataset type: %d", datasetType)
	}

	return writer.Write(header)
}

func (l *Listener) writeRecordToCSV(writer *csv.Writer, record protocol.Record) error {
	var row []string

	switch r := record.(type) {
	case protocol.Q1Record:
		row = []string{r.TransactionID, r.FinalAmount}
	case protocol.Q2Record:
		row = []string{r.YearMonthCreatedAt, r.ItemName, r.SellingsQty}
	case protocol.Q3Record:
		row = []string{r.YearHalfCreatedAt, r.StoreName, r.TPV}
	case protocol.Q4Record:
		row = []string{r.StoreName, r.Birthdate}
	default:
		return fmt.Errorf("unsupported record type: %T", record)
	}

	return writer.Write(row)
}
