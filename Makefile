SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/7574-sistemas-distribuidos/docker-compose-init

CLIENT_NUM ?= 1

default: build

all:

deps:
	go mod tidy
	go mod vendor

build: deps
	GOOS=linux go build -o bin/client github.com/7574-sistemas-distribuidos/docker-compose-init/client
.PHONY: build

docker-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up:
	@sed -i.bak 's|id: ".*"|id: "client_$(CLIENT_NUM)"|' config.yaml
	@sed -i.bak 's|transactionItems: ".*"|transactionItems: "./datasets/transaction_items/"|' config.yaml
	@sed -i.bak 's|transactions: ".*"|transactions: "./datasets/transactions/"|' config.yaml
	@rm -f config.yaml.bak
	@echo "Starting client with ID: client_$(CLIENT_NUM) (container: client-$(CLIENT_NUM))"
	CLIENT_NUM=$(CLIENT_NUM) CLI_ID=client_$(CLIENT_NUM) docker compose -p client-$(CLIENT_NUM) up --build
.PHONY: docker-compose-up

docker-compose-up-short:
	@sed -i.bak 's|id: ".*"|id: "client_$(CLIENT_NUM)"|' config.yaml
	@sed -i.bak 's|transactionItems: ".*"|transactionItems: "./datasets/transaction_items_short/"|' config.yaml
	@sed -i.bak 's|transactions: ".*"|transactions: "./datasets/transactions_short/"|' config.yaml
	@rm -f config.yaml.bak
	@echo "Starting client with ID: client_$(CLIENT_NUM) (SHORT datasets, container: client-$(CLIENT_NUM))"
	CLIENT_NUM=$(CLIENT_NUM) CLI_ID=client_$(CLIENT_NUM) docker compose -p client-$(CLIENT_NUM) up --build
.PHONY: docker-compose-up-short

docker-compose-down:
	CLIENT_NUM=$(CLIENT_NUM) docker compose -p client-$(CLIENT_NUM) -f docker-compose.yaml stop -t 1
	CLIENT_NUM=$(CLIENT_NUM) docker compose -p client-$(CLIENT_NUM) -f docker-compose.yaml down
	@sed -i.bak 's|id: ".*"|id: "client_1"|' config.yaml
	@sed -i.bak 's|transactionItems: ".*"|transactionItems: "./datasets/transaction_items/"|' config.yaml
	@sed -i.bak 's|transactions: ".*"|transactions: "./datasets/transactions/"|' config.yaml
	@rm -f config.yaml.bak
.PHONY: docker-compose-down

# Example scenarios - Launch clients with different example datasets
example-1-up:
	@cp config.yaml config_example_1.yaml
	@sed -i.bak 's|id: ".*"|id: "client_1"|' config_example_1.yaml
	@sed -i.bak 's|transactionItems: ".*"|transactionItems: "./datasets/transaction_items_example_1/"|' config_example_1.yaml
	@sed -i.bak 's|transactions: ".*"|transactions: "./datasets/transactions_example_1/"|' config_example_1.yaml
	@rm -f config_example_1.yaml.bak
	@echo "Starting EXAMPLE 1: client_1 (container: client-1, datasets: example_1)"
	CLIENT_NUM=1 CLI_ID=client_1 CONFIG_FILE=config_example_1.yaml docker compose -p client-1 up --build
.PHONY: example-1-up

example-2-up:
	@cp config.yaml config_example_2.yaml
	@sed -i.bak 's|id: ".*"|id: "client_2"|' config_example_2.yaml
	@sed -i.bak 's|transactionItems: ".*"|transactionItems: "./datasets/transaction_items_example_2/"|' config_example_2.yaml
	@sed -i.bak 's|transactions: ".*"|transactions: "./datasets/transactions_example_2/"|' config_example_2.yaml
	@rm -f config_example_2.yaml.bak
	@echo "Starting EXAMPLE 2: client_2 (container: client-2, datasets: example_2)"
	CLIENT_NUM=2 CLI_ID=client_2 CONFIG_FILE=config_example_2.yaml docker compose -p client-2 up --build
.PHONY: example-2-up

example-3-up:
	@cp config.yaml config_example_3.yaml
	@sed -i.bak 's|id: ".*"|id: "client_3"|' config_example_3.yaml
	@sed -i.bak 's|transactionItems: ".*"|transactionItems: "./datasets/transaction_items_example_3/"|' config_example_3.yaml
	@sed -i.bak 's|transactions: ".*"|transactions: "./datasets/transactions_example_3/"|' config_example_3.yaml
	@rm -f config_example_3.yaml.bak
	@echo "Starting EXAMPLE 3: client_3 (container: client-3, datasets: example_3)"
	CLIENT_NUM=3 CLI_ID=client_3 CONFIG_FILE=config_example_3.yaml docker compose -p client-3 up --build
.PHONY: example-3-up

example-4-up:
	@cp config.yaml config_example_4.yaml
	@sed -i.bak 's|id: ".*"|id: "client_4"|' config_example_4.yaml
	@sed -i.bak 's|transactionItems: ".*"|transactionItems: "./datasets/transaction_items_example_4/"|' config_example_4.yaml
	@sed -i.bak 's|transactions: ".*"|transactions: "./datasets/transactions_example_4/"|' config_example_4.yaml
	@rm -f config_example_4.yaml.bak
	@echo "Starting EXAMPLE 4: client_4 (container: client-4, datasets: example_4)"
	CLIENT_NUM=4 CLI_ID=client_4 CONFIG_FILE=config_example_4.yaml docker compose -p client-4 up --build
.PHONY: example-4-up

example-5-up:
	@cp config.yaml config_example_5.yaml
	@sed -i.bak 's|id: ".*"|id: "client_5"|' config_example_5.yaml
	@sed -i.bak 's|transactionItems: ".*"|transactionItems: "./datasets/transaction_items_example_5/"|' config_example_5.yaml
	@sed -i.bak 's|transactions: ".*"|transactions: "./datasets/transactions_example_5/"|' config_example_5.yaml
	@rm -f config_example_5.yaml.bak
	@echo "Starting EXAMPLE 5: client_5 (container: client-5, datasets: example_5)"
	CLIENT_NUM=5 CLI_ID=client_5 CONFIG_FILE=config_example_5.yaml docker compose -p client-5 up --build
.PHONY: example-5-up

example-down:
	@echo "Stopping all example clients..."
	@docker compose -p client-1 -f docker-compose.yaml stop -t 1 2>/dev/null || true
	@docker compose -p client-1 -f docker-compose.yaml down 2>/dev/null || true
	@docker compose -p client-2 -f docker-compose.yaml stop -t 1 2>/dev/null || true
	@docker compose -p client-2 -f docker-compose.yaml down 2>/dev/null || true
	@docker compose -p client-3 -f docker-compose.yaml stop -t 1 2>/dev/null || true
	@docker compose -p client-3 -f docker-compose.yaml down 2>/dev/null || true
	@rm -f config_example_1.yaml config_example_2.yaml config_example_3.yaml
	@sed -i.bak 's|id: ".*"|id: "client_1"|' config.yaml
	@sed -i.bak 's|transactionItems: ".*"|transactionItems: "./datasets/transaction_items/"|' config.yaml
	@sed -i.bak 's|transactions: ".*"|transactions: "./datasets/transactions/"|' config.yaml
	@rm -f config.yaml.bak
	@echo "All example clients stopped, temporary configs removed, and main config reset"
.PHONY: example-down

clean:
	@rm -rf output/*
	@echo "Output directory cleaned successfully"
.PHONY: clean

sort-answers:
	@echo "Sorting answers directory..."
	@python3 scripts/sort_results.py answers
.PHONY: sort-answers

sort-output:
	@echo "Sorting output for client $(CLIENT_NUM)..."
	@python3 scripts/sort_results.py output/client_$(CLIENT_NUM)
.PHONY: sort-output

compare-results:
	@echo "Comparing results for client $(CLIENT_NUM) against answers..."
	@python3 scripts/compare_results.py answers output/client_$(CLIENT_NUM)
.PHONY: compare-results

compare-results-diff:
	@echo "Comparing results for client $(CLIENT_NUM) against answers (with diff)..."
	@python3 scripts/compare_results.py answers output/client_$(CLIENT_NUM) --show-diff
.PHONY: compare-results-diff

run:
	@./scripts/run_and_validate.sh $(CLIENT_NUM) normal
.PHONY: run

run-short:
	@./scripts/run_and_validate.sh $(CLIENT_NUM) short
.PHONY: run-short

help:
	@echo "Available commands:"
	@echo ""
	@echo "Basic execution:"
	@echo "  make docker-compose-up              - Run with NORMAL datasets (default CLIENT_NUM=1)"
	@echo "  make docker-compose-up-short        - Run with SHORT datasets (default CLIENT_NUM=1)"
	@echo "  make docker-compose-down            - Stop and clean up containers (default CLIENT_NUM=1)"
	@echo "  make docker-compose-logs            - Follow container logs (default CLIENT_NUM=1)"
	@echo "  make clean                          - Clean all files from output directory"
	@echo ""
	@echo "🚀 Automated execution with validation:"
	@echo "  make run-and-validate CLIENT_NUM=1  - Run client + auto sort + auto compare"
	@echo "  make run-and-validate-short CLIENT_NUM=1 - Run with SHORT datasets + validation"
	@echo ""
	@echo "Manual validation:"
	@echo "  make sort-answers                   - Sort CSV files in answers directory"
	@echo "  make sort-output CLIENT_NUM=1       - Sort CSV files in output/client_1 directory"
	@echo "  make compare-results CLIENT_NUM=1   - Compare sorted results with answers"
	@echo "  make compare-results-diff CLIENT_NUM=1  - Compare with detailed diff output"
	@echo ""
	@echo "Multi-client testing (each client runs in isolated containers):"
	@echo "  make docker-compose-up CLIENT_NUM=1        - Run as client_1 (container: client-1, output: ./output/client_1/)"
	@echo "  make docker-compose-up CLIENT_NUM=2        - Run as client_2 (container: client-2, output: ./output/client_2/)"
	@echo "  make docker-compose-up-short CLIENT_NUM=3  - Run as client_3 (container: client-3, output: ./output/client_3/)"
	@echo ""
	@echo "  make docker-compose-down CLIENT_NUM=2      - Stop client-2 container"
	@echo "  make docker-compose-logs CLIENT_NUM=3      - View logs for client-3"
	@echo ""
	@echo "Example scenarios (different datasets for comparison):"
	@echo "  make example-1-up                          - Launch client_1 with example_1 datasets"
	@echo "  make example-2-up                          - Launch client_2 with example_2 datasets"
	@echo "  make example-3-up                          - Launch client_3 with example_3 datasets"
	@echo "  make example-down                          - Stop all example clients and reset config"
	@echo ""
	@echo "Quick multi-client test:"
	@echo "  Terminal 1: make example-1-up"
	@echo "  Terminal 2: make example-2-up"
	@echo "  Terminal 3: make example-3-up"
	@echo "  Compare results in: ./output/client_1/, ./output/client_2/, ./output/client_3/"
	@echo ""
.PHONY: help

docker-compose-logs:
	CLIENT_NUM=$(CLIENT_NUM) docker compose -p client-$(CLIENT_NUM) -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs

# COMMANDS:

# make docker-compose-up-short CLIENT_NUM=1

# make example-1-up

# make docker-compose-up CLIENT_NUM=1

# make run CLIENT_NUM=1
