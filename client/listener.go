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

	// Special handling for Q2 - separate writers for best selling and most profits
	var q2BestSellingWriter *csv.Writer
	var q2MostProfitsWriter *csv.Writer
	var q2BestSellingFile *os.File
	var q2MostProfitsFile *os.File

	defer func() {
		for _, writer := range csvWriters {
			writer.Flush()
		}
		for _, file := range outputFiles {
			file.Close()
		}
		// Close Q2 special files
		if q2BestSellingWriter != nil {
			q2BestSellingWriter.Flush()
		}
		if q2MostProfitsWriter != nil {
			q2MostProfitsWriter.Flush()
		}
		if q2BestSellingFile != nil {
			q2BestSellingFile.Close()
		}
		if q2MostProfitsFile != nil {
			q2MostProfitsFile.Close()
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

		// Validate it's a query response type (6, 9, 12, 16)
		if datasetType != protocol.DatasetQ1 && datasetType != protocol.DatasetQ2 &&
			datasetType != protocol.DatasetQ3 && datasetType != protocol.DatasetQ4 {
			return fmt.Errorf("received invalid dataset type for query response: %d", datasetType)
		}

		// Handle Q2 specially - create separate files for best selling and most profits
		if datasetType == protocol.DatasetQ2 {
			if q2BestSellingWriter == nil {
				// Initialize Q2 Best Selling CSV
				file1, writer1, err := l.createQ2BestSellingFile()
				if err != nil {
					return fmt.Errorf("failed to create Q2 best selling file: %w", err)
				}
				q2BestSellingFile = file1
				q2BestSellingWriter = writer1

				// Initialize Q2 Most Profits CSV
				file2, writer2, err := l.createQ2MostProfitsFile()
				if err != nil {
					return fmt.Errorf("failed to create Q2 most profits file: %w", err)
				}
				q2MostProfitsFile = file2
				q2MostProfitsWriter = writer2

				l.log.Infof("action: receive_query | result: in_progress | client_id: %v | query: Q2", l.clientID)
			}
		} else {
			// Initialize CSV writer if this is the first batch for this query
			if _, exists := csvWriters[datasetType]; !exists {
				file, writer, err := l.createOutputFile(datasetType)
				if err != nil {
					return fmt.Errorf("failed to create output file for query %d: %w", datasetType, err)
				}
				csvWriters[datasetType] = writer
				outputFiles[datasetType] = file

				var queryNum int
				switch datasetType {
				case protocol.DatasetQ1:
					queryNum = 1
				case protocol.DatasetQ2:
					queryNum = 2
				case protocol.DatasetQ3:
					queryNum = 3
				case protocol.DatasetQ4:
					queryNum = 4
				}
				l.log.Infof("action: receive_query | result: in_progress | client_id: %v | query: Q%d", l.clientID, queryNum)
			}
		}

		// Handle writing records - Q2 needs special handling
		if datasetType == protocol.DatasetQ2 {
			// Write Q2 records to separate files
			for i, record := range batchMessage.Records {
				switch r := record.(type) {
				case *protocol.Q2BestSellingWithNameRecord:
					if err := l.writeRecordToQ2BestSelling(q2BestSellingWriter, r); err != nil {
						return fmt.Errorf("failed to write Q2 best selling record: %w", err)
					}
					l.log.Debugf("action: write_q2_best_selling | client_id: %v | record_index: %d | year_month: %s | item_name: %s | qty: %s",
						l.clientID, i, r.YearMonth, r.ItemName, r.SellingsQty)
				case *protocol.Q2MostProfitsWithNameRecord:
					if err := l.writeRecordToQ2MostProfits(q2MostProfitsWriter, r); err != nil {
						return fmt.Errorf("failed to write Q2 most profits record: %w", err)
					}
					l.log.Debugf("action: write_q2_most_profits | client_id: %v | record_index: %d | year_month: %s | item_name: %s | profit: %s",
						l.clientID, i, r.YearMonth, r.ItemName, r.ProfitSum)
				default:
					l.log.Errorf("action: write_q2_record | result: error | client_id: %v | msg: unexpected record type: %T", l.clientID, record)
				}
			}

			// Flush both Q2 writers
			q2BestSellingWriter.Flush()
			q2MostProfitsWriter.Flush()

			l.log.Debugf("action: receive_batch | result: success | client_id: %v | query: Q2 | records_in_batch: %d",
				l.clientID, len(batchMessage.Records))
		} else {
			// Handle other queries normally
			csvWriter := csvWriters[datasetType]
			var queryNum int
			switch datasetType {
			case protocol.DatasetQ1:
				queryNum = 1
			case protocol.DatasetQ3:
				queryNum = 3
			case protocol.DatasetQ4:
				queryNum = 4
			}

			for i, record := range batchMessage.Records {
				if err := l.writeRecordToCSV(csvWriter, record); err != nil {
					return fmt.Errorf("failed to write record for query %d: %w", datasetType, err)
				}
				l.log.Debugf("action: write_record | client_id: %v | query: Q%d | record_index: %d | record_type: %T",
					l.clientID, queryNum, i, record)
			}

			csvWriter.Flush()
		}

		// if batchMessage.EOF {
		// 	queriesCompleted[datasetType] = true
		// 	var queryNum int
		// 	switch datasetType {
		// 	case protocol.DatasetQ1:
		// 		queryNum = 1
		// 	case protocol.DatasetQ2:
		// 		queryNum = 2
		// 	case protocol.DatasetQ3:
		// 		queryNum = 3
		// 	case protocol.DatasetQ4:
		// 		queryNum = 4
		// 	}
		// 	l.log.Infof("action: receive_query | result: complete | client_id: %v | query: Q%d", l.clientID, queryNum)
		// }
	}

	l.log.Infof("action: receive_queries | result: success | client_id: %v | msg: all queries received", l.clientID)
	return nil
}

// Creates a CSV file for the specified dataset type
func (l *Listener) createOutputFile(datasetType protocol.DatasetType) (*os.File, *csv.Writer, error) {
	var queryNum int
	switch datasetType {
	case protocol.DatasetQ1: // 6
		queryNum = 1
	case protocol.DatasetQ2: // 9
		queryNum = 2
	case protocol.DatasetQ3: // 12
		queryNum = 3
	case protocol.DatasetQ4: // 16
		queryNum = 4
	default:
		return nil, nil, fmt.Errorf("unknown query dataset type: %d", datasetType)
	}
	filename := fmt.Sprintf("Q%d_results.csv", queryNum)
	filepath := filepath.Join(l.outputDir, filename)

	if err := os.MkdirAll(l.outputDir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	file, err := os.Create(filepath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create file %s: %w", filepath, err)
	}

	l.log.Infof("action: create_output_file | result: success | client_id: %v | file: %s", l.clientID, filepath)

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
	case protocol.DatasetQ1: // 6
		header = []string{"transaction_id", "final_amount"}
	case protocol.DatasetQ2: // 9
		header = []string{"year_month", "item_name", "value"}
	case protocol.DatasetQ3: // 12
		header = []string{"year_half", "store_name", "tpv"}
	case protocol.DatasetQ4: // 16
		header = []string{"store_name", "birthdate"}
	default:
		return fmt.Errorf("unknown dataset type: %d", datasetType)
	}

	return writer.Write(header)
}

func (l *Listener) writeRecordToCSV(writer *csv.Writer, record protocol.Record) error {
	var row []string

	switch r := record.(type) {
	// Q1 Records
	case *protocol.Q1Record:
		row = []string{r.TransactionID, r.FinalAmount}

	// Q2 Records - Both types can appear in the same batch
	case *protocol.Q2BestSellingWithNameRecord:
		row = []string{r.YearMonth, r.ItemName, r.SellingsQty}
	case *protocol.Q2MostProfitsWithNameRecord:
		row = []string{r.YearMonth, r.ItemName, r.ProfitSum}

	// Q3 Records
	case *protocol.Q3JoinedRecord:
		row = []string{r.YearHalf, r.StoreName, r.TPV}

	// Q4 Records
	case *protocol.Q4JoinedWithStoreAndUserRecord:
		row = []string{r.StoreName, r.Birthdate}

	default:
		return fmt.Errorf("unsupported record type: %T", record)
	}

	return writer.Write(row)
}

// createQ2BestSellingFile creates the CSV file for Q2 best selling records
func (l *Listener) createQ2BestSellingFile() (*os.File, *csv.Writer, error) {
	filename := "Q2_best_selling.csv"
	filepath := filepath.Join(l.outputDir, filename)

	if err := os.MkdirAll(l.outputDir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	file, err := os.Create(filepath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create file %s: %w", filepath, err)
	}

	csvWriter := csv.NewWriter(file)

	// Write header for best selling: year_month, item_name, sellings_qty
	header := []string{"year_month", "item_name", "sellings_qty"}
	if err := csvWriter.Write(header); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to write CSV header: %w", err)
	}

	l.log.Infof("action: create_q2_best_selling_file | result: success | client_id: %v | file: %s", l.clientID, filepath)
	return file, csvWriter, nil
}

// createQ2MostProfitsFile creates the CSV file for Q2 most profits records
func (l *Listener) createQ2MostProfitsFile() (*os.File, *csv.Writer, error) {
	filename := "Q2_most_profits.csv"
	filepath := filepath.Join(l.outputDir, filename)

	if err := os.MkdirAll(l.outputDir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	file, err := os.Create(filepath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create file %s: %w", filepath, err)
	}

	csvWriter := csv.NewWriter(file)

	// Write header for most profits: year_month, item_name, profit_sum
	header := []string{"year_month", "item_name", "profit_sum"}
	if err := csvWriter.Write(header); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to write CSV header: %w", err)
	}

	l.log.Infof("action: create_q2_most_profits_file | result: success | client_id: %v | file: %s", l.clientID, filepath)
	return file, csvWriter, nil
}

// writeRecordToQ2BestSelling writes a Q2BestSellingWithNameRecord to the best selling CSV
func (l *Listener) writeRecordToQ2BestSelling(writer *csv.Writer, record *protocol.Q2BestSellingWithNameRecord) error {
	row := []string{record.YearMonth, record.ItemName, record.SellingsQty}
	return writer.Write(row)
}

// writeRecordToQ2MostProfits writes a Q2MostProfitsWithNameRecord to the most profits CSV
func (l *Listener) writeRecordToQ2MostProfits(writer *csv.Writer, record *protocol.Q2MostProfitsWithNameRecord) error {
	row := []string{record.YearMonth, record.ItemName, record.ProfitSum}
	return writer.Write(row)
}
