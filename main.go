package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/distribuidos-Coffee-Shop-Analysis/client/client"
	"github.com/distribuidos-Coffee-Shop-Analysis/client/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/client/protocol"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("batch", "maxAmount")
	v.BindEnv("id")
	v.BindEnv("server", "address")
	v.BindEnv("loop", "period")
	v.BindEnv("loop", "amount")
	v.BindEnv("log", "level")
	v.BindEnv("output", "dir")

	// Dataset paths
	v.BindEnv("datasets", "menuItems")
	v.BindEnv("datasets", "stores")
	v.BindEnv("datasets", "transactionItems")
	v.BindEnv("datasets", "transactions")
	v.BindEnv("datasets", "users")

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	// Parse time.Duration variables and return an error if those variables cannot be parsed

	if _, err := time.ParseDuration(v.GetString("loop.period")); err != nil {
		return nil, errors.Wrapf(err, "Could not parse CLI_LOOP_PERIOD env var as time.Duration.")
	}

	return v, nil
}

// InitLogger Receives the log level to be set in go-logging as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	// Set the backends to be used.
	logging.SetBackend(backendLeveled)
	return nil
}

// PrintConfig Print all the configuration parameters of the program.
// For debugging purposes only
func PrintConfig(v *viper.Viper) {
	log.Infof("action: config | result: success | client_id: %s | server_address: %s | loop_amount: %v | loop_period: %v | log_level: %s",
		v.GetString("id"),
		v.GetString("server.address"),
		v.GetInt("loop.amount"),
		v.GetDuration("loop.period"),
		v.GetString("log.level"),
	)
}

func main() {
	v, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
	}

	// Print program config with debugging purposes
	PrintConfig(v)

	datasetPaths := map[protocol.DatasetType]string{
		protocol.DatasetMenuItems:        v.GetString("datasets.menuItems"),
		protocol.DatasetStores:           v.GetString("datasets.stores"),
		protocol.DatasetTransactionItems: v.GetString("datasets.transactionItems"),
		protocol.DatasetTransactions:     v.GetString("datasets.transactions"),
		protocol.DatasetUsers:            v.GetString("datasets.users"),
	}

	clientConfig := client.ClientConfig{
		ServerAddress:  v.GetString("server.address"),
		ID:             v.GetString("id"),
		LoopAmount:     v.GetInt("loop.amount"),
		LoopPeriod:     v.GetDuration("loop.period"),
		BatchMaxAmount: v.GetInt("batch.maxAmount"),
		DatasetPaths:   datasetPaths,
		OutputDir:      v.GetString("output.dir"),
	}

	// Set default batch size if not configured or is less than 2
	if clientConfig.BatchMaxAmount < 2 {
		clientConfig.BatchMaxAmount = common.DEFAULT_BATCH_MAX_AMOUNT
		log.Infof("action: config | result: default_batch_size | batch_max_amount: %d", clientConfig.BatchMaxAmount)
	}

	// Set output directory with client-specific subdirectory
	baseOutputDir := clientConfig.OutputDir
	if baseOutputDir == "" {
		baseOutputDir = "./output"
	}
	// Append client ID to create per-client output directory (e.g., ./output/client_001/)
	clientConfig.OutputDir = filepath.Join(baseOutputDir, clientConfig.ID)
	log.Infof("action: config | result: output_dir | path: %s", clientConfig.OutputDir)

	client := client.NewClient(clientConfig)
	if client == nil {
		log.Criticalf("action: init_client | result: fail | msg: failed to initialize client")
		return
	}

	client.StartClientWithDatasets()

}
