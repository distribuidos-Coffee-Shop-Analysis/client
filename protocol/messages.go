package protocol

import (
	"fmt"
	"strings"
)

type BatchMessage struct {
	Type        MessageType
	DatasetType DatasetType
	BatchIndex  int
	Records     []Record
	EOF         bool
}

type MessageType byte

const (
	MessageTypeBatch    MessageType = 1
	MessageTypeResponse MessageType = 2
)

type DatasetType byte

const (
	// Input datasets
	DatasetMenuItems        DatasetType = 1
	DatasetStores           DatasetType = 2
	DatasetTransactionItems DatasetType = 3
	DatasetTransactions     DatasetType = 4
	DatasetUsers            DatasetType = 5

	DatasetQ1 DatasetType = 6  // Q1 final results
	DatasetQ2 DatasetType = 9  // Q2AggWithName - final Q2 results with item names
	DatasetQ3 DatasetType = 12 // Q3AggWithName - final Q3 results with store names
	DatasetQ4 DatasetType = 16 // Q4AggWithUserAndStore - final Q4 results
)

// Record interface that all dataset records must implement
type Record interface {
	Serialize() string
	GetType() DatasetType
}

type MenuItemRecord struct {
	ItemID        string
	ItemName      string
	Category      string
	Price         string
	IsSeasonal    string
	AvailableFrom string
	AvailableTo   string
}

func (m MenuItemRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s",
		m.ItemID, m.ItemName, m.Category, m.Price, m.IsSeasonal, m.AvailableFrom, m.AvailableTo)
}

func (m MenuItemRecord) GetType() DatasetType {
	return DatasetMenuItems
}

type StoreRecord struct {
	StoreID    string
	StoreName  string
	Street     string
	PostalCode string
	City       string
	State      string
	Latitude   string
	Longitude  string
}

func (s StoreRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s",
		s.StoreID, s.StoreName, s.Street, s.PostalCode, s.City, s.State, s.Latitude, s.Longitude)
}

func (s StoreRecord) GetType() DatasetType {
	return DatasetStores
}

type TransactionItemRecord struct {
	TransactionID string
	ItemID        string
	Quantity      string
	UnitPrice     string
	Subtotal      string
	CreatedAt     string
}

func (t TransactionItemRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s",
		t.TransactionID, t.ItemID, t.Quantity, t.UnitPrice, t.Subtotal, t.CreatedAt)
}

func (t TransactionItemRecord) GetType() DatasetType {
	return DatasetTransactionItems
}

type TransactionRecord struct {
	TransactionID   string
	StoreID         string
	PaymentMethodID string
	VoucherID       string
	UserID          string
	OriginalAmount  string
	DiscountApplied string
	FinalAmount     string
	CreatedAt       string
}

func (t TransactionRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s",
		t.TransactionID, t.StoreID, t.PaymentMethodID, t.VoucherID, t.UserID,
		t.OriginalAmount, t.DiscountApplied, t.FinalAmount, t.CreatedAt)
}

func (t TransactionRecord) GetType() DatasetType {
	return DatasetTransactions
}

type UserRecord struct {
	UserID       string
	Gender       string
	Birthdate    string
	RegisteredAt string
}

func (u UserRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s|%s",
		u.UserID, u.Gender, u.Birthdate, u.RegisteredAt)
}

func (u UserRecord) GetType() DatasetType {
	return DatasetUsers
}

// Query response records

// Q1Record represents Q1 record: transaction_id, final_amount
type Q1Record struct {
	TransactionID string
	FinalAmount   string
}

const Q1RecordParts = 2

// Serialize returns the string representation of the Q1 record
func (q *Q1Record) Serialize() string {
	return fmt.Sprintf("%s|%s", q.TransactionID, q.FinalAmount)
}

// GetType returns the dataset type for Q1 records
func (q *Q1Record) GetType() DatasetType {
	return DatasetQ1
}

// NewQ1RecordFromString creates a Q1Record from a string
func NewQ1RecordFromString(data string) (*Q1Record, error) {
	parts := strings.Split(data, "|")
	return NewQ1RecordFromParts(parts)
}

// NewQ1RecordFromParts creates a Q1Record from string parts
func NewQ1RecordFromParts(parts []string) (*Q1Record, error) {
	if len(parts) < Q1RecordParts {
		return nil, fmt.Errorf("invalid Q1Record format: expected %d fields, got %d",
			Q1RecordParts, len(parts))
	}

	return &Q1Record{
		TransactionID: parts[0],
		FinalAmount:   parts[1],
	}, nil
}

// ===== Q2 Joined Records (output from Join node) =====

// Q2BestSellingWithNameRecord represents best selling items with names: year_month, item_name, sellings_qty
type Q2BestSellingWithNameRecord struct {
	YearMonth   string
	ItemName    string
	SellingsQty string
}

const Q2BestSellingWithNameRecordParts = 3

func (q *Q2BestSellingWithNameRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearMonth, q.ItemName, q.SellingsQty)
}

func (q *Q2BestSellingWithNameRecord) GetType() DatasetType {
	return DatasetQ2
}

func NewQ2BestSellingWithNameRecordFromParts(parts []string) (*Q2BestSellingWithNameRecord, error) {
	if len(parts) < Q2BestSellingWithNameRecordParts {
		return nil, fmt.Errorf("invalid Q2BestSellingWithNameRecord format: expected %d fields, got %d",
			Q2BestSellingWithNameRecordParts, len(parts))
	}
	return &Q2BestSellingWithNameRecord{
		YearMonth:   parts[0],
		ItemName:    parts[1],
		SellingsQty: parts[2],
	}, nil
}

// Q2MostProfitsWithNameRecord represents most profitable items with names: year_month, item_name, profit_sum
type Q2MostProfitsWithNameRecord struct {
	YearMonth string
	ItemName  string
	ProfitSum string
}

const Q2MostProfitsWithNameRecordParts = 3

func (q *Q2MostProfitsWithNameRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearMonth, q.ItemName, q.ProfitSum)
}

func (q *Q2MostProfitsWithNameRecord) GetType() DatasetType {
	return DatasetQ2
}

func NewQ2MostProfitsWithNameRecordFromParts(parts []string) (*Q2MostProfitsWithNameRecord, error) {
	if len(parts) < Q2MostProfitsWithNameRecordParts {
		return nil, fmt.Errorf("invalid Q2MostProfitsWithNameRecord format: expected %d fields, got %d",
			Q2MostProfitsWithNameRecordParts, len(parts))
	}
	return &Q2MostProfitsWithNameRecord{
		YearMonth: parts[0],
		ItemName:  parts[1],
		ProfitSum: parts[2],
	}, nil
}

// ===== Q3 Joined Records (output from Join node) =====

// Q3JoinedRecord represents Q3 joined data with store name: year_half_created_at, store_name, tpv
type Q3JoinedRecord struct {
	YearHalf  string
	StoreName string
	TPV       string
}

const Q3JoinedRecordParts = 3

func (q *Q3JoinedRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearHalf, q.StoreName, q.TPV)
}

func (q *Q3JoinedRecord) GetType() DatasetType {
	return DatasetQ3
}

func NewQ3JoinedRecordFromParts(parts []string) (*Q3JoinedRecord, error) {
	if len(parts) < Q3JoinedRecordParts {
		return nil, fmt.Errorf("invalid Q3JoinedRecord format: expected %d fields, got %d",
			Q3JoinedRecordParts, len(parts))
	}
	return &Q3JoinedRecord{
		YearHalf:  parts[0],
		StoreName: parts[1],
		TPV:       parts[2],
	}, nil
}

// ===== Q4 Final Joined Records (second join with stores) =====

// Q4JoinedWithStoreAndUserRecord represents final Q4 data: store_name, purchases_qty, birthdate
type Q4JoinedWithStoreAndUserRecord struct {
	StoreName    string
	PurchasesQty string
	Birthdate    string
}

const Q4JoinedWithStoreAndUserRecordParts = 3

func (q *Q4JoinedWithStoreAndUserRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.StoreName, q.PurchasesQty, q.Birthdate)
}

func (q *Q4JoinedWithStoreAndUserRecord) GetType() DatasetType {
	return DatasetQ4
}

func NewQ4JoinedWithStoreAndUserRecordFromParts(parts []string) (*Q4JoinedWithStoreAndUserRecord, error) {
	if len(parts) < Q4JoinedWithStoreAndUserRecordParts {
		return nil, fmt.Errorf("invalid Q4JoinedWithStoreAndUserRecord format: expected %d fields, got %d",
			Q4JoinedWithStoreAndUserRecordParts, len(parts))
	}
	return &Q4JoinedWithStoreAndUserRecord{
		StoreName:    parts[0],
		PurchasesQty: parts[1],
		Birthdate:    parts[2],
	}, nil
}

func NewBatchMessage(datasetType DatasetType, batchIndex int, records []Record, eof bool) *BatchMessage {
	return &BatchMessage{
		Type:        MessageTypeBatch,
		DatasetType: datasetType,
		BatchIndex:  batchIndex,
		Records:     records,
		EOF:         eof,
	}
}

// boolToInt converts boolean to integer
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
