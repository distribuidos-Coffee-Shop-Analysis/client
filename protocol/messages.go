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
)

// Record interface that all dataset records must implement
type Record interface {
	Serialize() string
	GetType() DatasetType
}

// represents a menu item dataset record
// Columns: item_id,item_name,category,price,is_seasonal,available_from,available_to
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

// represents a store dataset record
// Columns: store_id,store_name,street,postal_code,city,state,latitude,longitude
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

// represents a transaction item dataset record
// Columns: transaction_id,item_id,quantity,unit_price,subtotal,created_at
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

// represents a transaction dataset record
// Columns: transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at
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

// represents a user dataset record
// Columns: user_id,gender,birthdate,registered_at
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

type BatchMessage struct {
	Type        MessageType
	DatasetType DatasetType
	Records     []Record
	EOF         bool
}

// ResponseMessage represents server response to client
type ResponseMessage struct {
	Type    MessageType
	Success bool
	Error   string
}

func NewBatchMessage(datasetType DatasetType, records []Record, eof bool) *BatchMessage {
	return &BatchMessage{
		Type:        MessageTypeBatch,
		DatasetType: datasetType,
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
