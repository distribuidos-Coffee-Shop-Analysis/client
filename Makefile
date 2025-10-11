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

help:
	@echo "Available commands:"
	@echo ""
	@echo "  make docker-compose-up              - Run with NORMAL datasets (default CLIENT_NUM=1)"
	@echo "  make docker-compose-up-short        - Run with SHORT datasets (default CLIENT_NUM=1)"
	@echo "  make docker-compose-down            - Stop and clean up containers (default CLIENT_NUM=1)"
	@echo "  make docker-compose-logs            - Follow container logs (default CLIENT_NUM=1)"
	@echo ""
	@echo "Multi-client testing (each client runs in isolated containers):"
	@echo "  make docker-compose-up CLIENT_NUM=1        - Run as client_1 (container: client-1, output: ./output/client_1/)"
	@echo "  make docker-compose-up CLIENT_NUM=2        - Run as client_2 (container: client-2, output: ./output/client_2/)"
	@echo "  make docker-compose-up-short CLIENT_NUM=3  - Run as client_3 (container: client-3, output: ./output/client_3/)"
	@echo ""
	@echo "  make docker-compose-down CLIENT_NUM=2      - Stop client-2 container"
	@echo "  make docker-compose-logs CLIENT_NUM=3      - View logs for client-3"
	@echo ""
.PHONY: help

docker-compose-logs:
	CLIENT_NUM=$(CLIENT_NUM) docker compose -p client-$(CLIENT_NUM) -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs

# COMMANDS:
# make docker-compose-up-short CLIENT_NUM=1
# make docker-compose-up-short CLIENT_NUM=2
