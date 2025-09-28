package client

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/distribuidos-Coffee-Shop-Analysis/client/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/client/protocol"
)

// Handles reading and parsing CSV files for different dataset files
type FileManager struct {
	datasetType protocol.DatasetType
	file        *os.File
	reader      *csv.Reader
}

func NewMenuItemFileManager(csvFilePath string) (*FileManager, error) {
	return newFileManager(protocol.DatasetMenuItems, csvFilePath)
}

func NewStoreFileManager(csvFilePath string) (*FileManager, error) {
	return newFileManager(protocol.DatasetStores, csvFilePath)
}

func NewTransactionItemFileManager(csvFilePath string) (*FileManager, error) {
	return newFileManager(protocol.DatasetTransactionItems, csvFilePath)
}

func NewTransactionFileManager(csvFilePath string) (*FileManager, error) {
	return newFileManager(protocol.DatasetTransactions, csvFilePath)
}

func NewUserFileManager(csvFilePath string) (*FileManager, error) {
	return newFileManager(protocol.DatasetUsers, csvFilePath)
}

// Common constructor logic
func newFileManager(datasetType protocol.DatasetType, csvFilePath string) (*FileManager, error) {
	file, err := os.Open(csvFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s | %v", csvFilePath, err)
	}

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 0

	// Skip the header line
	_, err = reader.Read()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read header from file %s | %v", csvFilePath, err)
	}

	return &FileManager{
		datasetType: datasetType,
		file:        file,
		reader:      reader,
	}, nil
}

func (f *FileManager) NextRecord() (protocol.Record, bool, error) {
	for {
		line, err := f.reader.Read()
		if err != nil {
			if err == io.EOF {
				return nil, true, nil // EOF reached
			}
			return nil, false, fmt.Errorf("failed to read from file | %v", err)
		}

		// Create record based on dataset type
		record, err := f.createRecord(line)
		if err != nil {
			// Skip invalid records, continue to next
			continue
		}

		return record, false, nil
	}
}

func (f *FileManager) Close() {
	err := f.file.Close()
	if err != nil {
		_ = fmt.Errorf("failed to close file | %v", err)
	}
}

// Creates the appropriate record type based on dataset type
func (f *FileManager) createRecord(line []string) (protocol.Record, error) {
	switch f.datasetType {
	case protocol.DatasetMenuItems:
		return f.createMenuItemRecord(line)
	case protocol.DatasetStores:
		return f.createStoreRecord(line)
	case protocol.DatasetTransactionItems:
		return f.createTransactionItemRecord(line)
	case protocol.DatasetTransactions:
		return f.createTransactionRecord(line)
	case protocol.DatasetUsers:
		return f.createUserRecord(line)
	default:
		return nil, fmt.Errorf("unsupported dataset type: %d", f.datasetType)
	}
}

func (f *FileManager) createMenuItemRecord(record []string) (protocol.MenuItemRecord, error) {
	if len(record) != common.EXPECTED_MENU_ITEMS_FIELDS {
		return protocol.MenuItemRecord{}, fmt.Errorf("expected %d fields, got %d", common.EXPECTED_MENU_ITEMS_FIELDS, len(record))
	}

	for i := range record {
		record[i] = strings.TrimSpace(record[i])
		// available_from and available_to can be empty
		if record[i] == "" && i != common.CSV_AVAILABLE_FROM && i != common.CSV_AVAILABLE_TO {
			return protocol.MenuItemRecord{}, fmt.Errorf("field %d is empty", i)
		}
	}

	if len(record[common.CSV_ITEM_NAME]) > common.MAX_ITEM_NAME_BYTES {
		return protocol.MenuItemRecord{}, fmt.Errorf("item_name exceeds %d bytes: %d", common.MAX_ITEM_NAME_BYTES, len(record[common.CSV_ITEM_NAME]))
	}
	if len(record[common.CSV_CATEGORY]) > common.MAX_CATEGORY_BYTES {
		return protocol.MenuItemRecord{}, fmt.Errorf("category exceeds %d bytes: %d", common.MAX_CATEGORY_BYTES, len(record[common.CSV_CATEGORY]))
	}

	return protocol.MenuItemRecord{
		ItemID:        record[common.CSV_MENU_ITEM_ID],
		ItemName:      record[common.CSV_ITEM_NAME],
		Category:      record[common.CSV_CATEGORY],
		Price:         record[common.CSV_PRICE],
		IsSeasonal:    record[common.CSV_IS_SEASONAL],
		AvailableFrom: record[common.CSV_AVAILABLE_FROM],
		AvailableTo:   record[common.CSV_AVAILABLE_TO],
	}, nil
}

// createStoreRecord creates and validates a StoreRecord
func (f *FileManager) createStoreRecord(record []string) (protocol.StoreRecord, error) {
	if len(record) != common.EXPECTED_STORES_FIELDS {
		return protocol.StoreRecord{}, fmt.Errorf("expected %d fields, got %d", common.EXPECTED_STORES_FIELDS, len(record))
	}

	// Trim whitespace from all fields
	for i := range record {
		record[i] = strings.TrimSpace(record[i])
		if record[i] == "" {
			return protocol.StoreRecord{}, fmt.Errorf("field %d is empty", i)
		}
	}

	return protocol.StoreRecord{
		StoreID:    record[common.CSV_STORE_ID],
		StoreName:  record[common.CSV_STORE_NAME],
		Street:     record[common.CSV_STREET],
		PostalCode: record[common.CSV_POSTAL_CODE],
		City:       record[common.CSV_CITY],
		State:      record[common.CSV_STATE],
		Latitude:   record[common.CSV_LATITUDE],
		Longitude:  record[common.CSV_LONGITUDE],
	}, nil
}

func (f *FileManager) createTransactionItemRecord(record []string) (protocol.TransactionItemRecord, error) {
	if len(record) != common.EXPECTED_TRANSACTION_ITEMS_FIELDS {
		return protocol.TransactionItemRecord{}, fmt.Errorf("expected %d fields, got %d", common.EXPECTED_TRANSACTION_ITEMS_FIELDS, len(record))
	}

	// Trim whitespace from all fields
	for i := range record {
		record[i] = strings.TrimSpace(record[i])
		if record[i] == "" {
			return protocol.TransactionItemRecord{}, fmt.Errorf("field %d is empty", i)
		}
	}

	return protocol.TransactionItemRecord{
		TransactionID: record[common.CSV_TXN_ITEM_TRANSACTION_ID],
		ItemID:        record[common.CSV_TXN_ITEM_ITEM_ID],
		Quantity:      record[common.CSV_TXN_ITEM_QUANTITY],
		UnitPrice:     record[common.CSV_TXN_ITEM_UNIT_PRICE],
		Subtotal:      record[common.CSV_TXN_ITEM_SUBTOTAL],
		CreatedAt:     record[common.CSV_TXN_ITEM_CREATED_AT],
	}, nil
}

func (f *FileManager) createTransactionRecord(record []string) (protocol.TransactionRecord, error) {
	if len(record) != common.EXPECTED_TRANSACTIONS_FIELDS {
		return protocol.TransactionRecord{}, fmt.Errorf("expected %d fields, got %d", common.EXPECTED_TRANSACTIONS_FIELDS, len(record))
	}

	for i := range record {
		record[i] = strings.TrimSpace(record[i])
		// voucher_id and user_id can be empty
		if record[i] == "" && i != common.CSV_TXN_VOUCHER_ID && i != common.CSV_TXN_USER_ID {
			return protocol.TransactionRecord{}, fmt.Errorf("field %d is empty", i)
		}
	}

	return protocol.TransactionRecord{
		TransactionID:   record[common.CSV_TXN_TRANSACTION_ID],
		StoreID:         record[common.CSV_TXN_STORE_ID],
		PaymentMethodID: record[common.CSV_TXN_PAYMENT_METHOD_ID],
		VoucherID:       record[common.CSV_TXN_VOUCHER_ID],
		UserID:          record[common.CSV_TXN_USER_ID],
		OriginalAmount:  record[common.CSV_TXN_ORIGINAL_AMOUNT],
		DiscountApplied: record[common.CSV_TXN_DISCOUNT_APPLIED],
		FinalAmount:     record[common.CSV_TXN_FINAL_AMOUNT],
		CreatedAt:       record[common.CSV_TXN_CREATED_AT],
	}, nil
}

func (f *FileManager) createUserRecord(record []string) (protocol.UserRecord, error) {
	if len(record) != common.EXPECTED_USERS_FIELDS {
		return protocol.UserRecord{}, fmt.Errorf("expected %d fields, got %d", common.EXPECTED_USERS_FIELDS, len(record))
	}

	for i := range record {
		record[i] = strings.TrimSpace(record[i])
		if record[i] == "" {
			return protocol.UserRecord{}, fmt.Errorf("field %d is empty", i)
		}
	}

	return protocol.UserRecord{
		UserID:       record[common.CSV_USER_ID],
		Gender:       record[common.CSV_GENDER],
		Birthdate:    record[common.CSV_BIRTHDATE],
		RegisteredAt: record[common.CSV_REGISTERED_AT],
	}, nil
}
