SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/7574-sistemas-distribuidos/docker-compose-init

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
	@sed -i.bak 's|transactionItems: ".*"|transactionItems: "./datasets/transaction_items/"|' config.yaml
	@sed -i.bak 's|transactions: ".*"|transactions: "./datasets/transactions/"|' config.yaml
	@rm -f config.yaml.bak
	docker compose up --build
.PHONY: docker-compose-up

docker-compose-up-short:
	@sed -i.bak 's|transactionItems: ".*"|transactionItems: "./datasets/transaction_items_short/"|' config.yaml
	@sed -i.bak 's|transactions: ".*"|transactions: "./datasets/transactions_short/"|' config.yaml
	@rm -f config.yaml.bak
	docker compose up --build
.PHONY: docker-compose-up-short

docker-compose-down:
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
	@sed -i.bak 's|transactionItems: ".*"|transactionItems: "./datasets/transaction_items/"|' config.yaml
	@sed -i.bak 's|transactions: ".*"|transactions: "./datasets/transactions/"|' config.yaml
	@rm -f config.yaml.bak
.PHONY: docker-compose-down

help:
	@echo "Available commands:"
	@echo ""
	@echo "  make docker-compose-up        - Run with NORMAL datasets (full data)"
	@echo "  make docker-compose-up-short  - Run with SHORT datasets (reduced data for testing)"
	@echo "  make docker-compose-down      - Stop and clean up containers"
	@echo "  make docker-compose-logs      - Follow container logs"
	@echo ""
.PHONY: help

docker-compose-logs:
	docker compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs
