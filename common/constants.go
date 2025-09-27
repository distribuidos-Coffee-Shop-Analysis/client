package common

// Dataset field validation limits (in bytes)
const (
	// Menu Items dataset 
	EXPECTED_MENU_ITEMS_FIELDS = 7
	MAX_ITEM_ID_BYTES          = 20
	MAX_ITEM_NAME_BYTES        = 100
	MAX_CATEGORY_BYTES         = 50
	MAX_PRICE_BYTES            = 20
	MAX_IS_SEASONAL_BYTES      = 10
	MAX_AVAILABLE_DATE_BYTES   = 20

	// Stores dataset 
	EXPECTED_STORES_FIELDS = 8
	MAX_STORE_ID_BYTES     = 20
	MAX_STORE_NAME_BYTES   = 100
	MAX_STREET_BYTES       = 150
	MAX_POSTAL_CODE_BYTES  = 20
	MAX_CITY_BYTES         = 50
	MAX_STATE_BYTES        = 50
	MAX_COORDINATE_BYTES   = 20

	// Transaction Items dataset
	EXPECTED_TRANSACTION_ITEMS_FIELDS = 6
	MAX_TRANSACTION_ID_BYTES          = 20
	MAX_QUANTITY_BYTES                = 10
	MAX_UNIT_PRICE_BYTES              = 20
	MAX_SUBTOTAL_BYTES                = 20
	MAX_CREATED_AT_BYTES              = 30

	// Transactions dataset 
	EXPECTED_TRANSACTIONS_FIELDS = 9
	MAX_PAYMENT_METHOD_ID_BYTES  = 20
	MAX_VOUCHER_ID_BYTES         = 20
	MAX_USER_ID_BYTES            = 20
	MAX_AMOUNT_BYTES             = 20

	// Users dataset 
	EXPECTED_USERS_FIELDS   = 4
	MAX_GENDER_BYTES        = 10
	MAX_BIRTHDATE_BYTES     = 20
	MAX_REGISTERED_AT_BYTES = 30

	// Batch processing limits
	MAX_BATCH_SIZE_BYTES     = 8 * 1024 // 8KB limit
	DEFAULT_BATCH_MAX_AMOUNT = 10       // Default batch size when not configured
	MAX_SEND_RETRIES         = 3        // Maximum retries for sending messages

	// CSV processing limits
	MAX_CSV_RECORDS = 100000 // Maximum number of valid records to process from CSV

	// Protocol constants
	LENGTH_PREFIX_BYTES = 4 // Length prefix size in bytes
)

// Indices
const (
	CSV_MENU_ITEM_ID   = 0
	CSV_ITEM_NAME      = 1
	CSV_CATEGORY       = 2
	CSV_PRICE          = 3
	CSV_IS_SEASONAL    = 4
	CSV_AVAILABLE_FROM = 5
	CSV_AVAILABLE_TO   = 6
)

const (
	CSV_STORE_ID    = 0
	CSV_STORE_NAME  = 1
	CSV_STREET      = 2
	CSV_POSTAL_CODE = 3
	CSV_CITY        = 4
	CSV_STATE       = 5
	CSV_LATITUDE    = 6
	CSV_LONGITUDE   = 7
)

const (
	CSV_TXN_ITEM_TRANSACTION_ID = 0
	CSV_TXN_ITEM_ITEM_ID        = 1
	CSV_TXN_ITEM_QUANTITY       = 2
	CSV_TXN_ITEM_UNIT_PRICE     = 3
	CSV_TXN_ITEM_SUBTOTAL       = 4
	CSV_TXN_ITEM_CREATED_AT     = 5
)

const (
	CSV_TXN_TRANSACTION_ID    = 0
	CSV_TXN_STORE_ID          = 1
	CSV_TXN_PAYMENT_METHOD_ID = 2
	CSV_TXN_VOUCHER_ID        = 3
	CSV_TXN_USER_ID           = 4
	CSV_TXN_ORIGINAL_AMOUNT   = 5
	CSV_TXN_DISCOUNT_APPLIED  = 6
	CSV_TXN_FINAL_AMOUNT      = 7
	CSV_TXN_CREATED_AT        = 8
)

const (
	CSV_USER_ID       = 0
	CSV_GENDER        = 1
	CSV_BIRTHDATE     = 2
	CSV_REGISTERED_AT = 3
)
