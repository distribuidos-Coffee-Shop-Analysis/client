package protocol

import "fmt"

type MessageType byte

const (
	MessageTypeBatch    MessageType = 1
	MessageTypeResponse MessageType = 2
)

type DatasetType byte

const (
	DatasetMenuItems        DatasetType = 1
	DatasetStores           DatasetType = 2
	DatasetTransactionItems DatasetType = 3
	DatasetTransactions     DatasetType = 4
	DatasetUsers            DatasetType = 5
	DatasetQ1               DatasetType = 6
	DatasetQ2               DatasetType = 7
	DatasetQ3               DatasetType = 8
	DatasetQ4               DatasetType = 9
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

// Q1:  transaction_id, final_amount
type Q1Record struct {
	TransactionID string
	FinalAmount   string
}

func (q Q1Record) Serialize() string {
	return fmt.Sprintf("%s|%s", q.TransactionID, q.FinalAmount)
}

func (q Q1Record) GetType() DatasetType {
	return DatasetQ1
}

// Q2: year_month_created_at, item_name, sellings_qty
type Q2Record struct {
	YearMonthCreatedAt string
	ItemName           string
	SellingsQty        string
}

func (q Q2Record) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearMonthCreatedAt, q.ItemName, q.SellingsQty)
}

func (q Q2Record) GetType() DatasetType {
	return DatasetQ2
}

// Q3: year_half_created_at, store_name, tpv
type Q3Record struct {
	YearHalfCreatedAt string
	StoreName         string
	TPV               string
}

func (q Q3Record) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearHalfCreatedAt, q.StoreName, q.TPV)
}

func (q Q3Record) GetType() DatasetType {
	return DatasetQ3
}

// Q4: store_name, birthdate
type Q4Record struct {
	StoreName string
	Birthdate string
}

func (q Q4Record) Serialize() string {
	return fmt.Sprintf("%s|%s", q.StoreName, q.Birthdate)
}

func (q Q4Record) GetType() DatasetType {
	return DatasetQ4
}

type BatchMessage struct {
	Type        MessageType
	DatasetType DatasetType
	BatchIndex  int
	Records     []Record
	EOF         bool
}

// ResponseMessage represents server response to client
type ResponseMessage struct {
	Type    MessageType
	Success bool
	Error   string
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
