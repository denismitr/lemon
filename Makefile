DB_VERSION ?= 0.1
GO_VERSION ?= 1.16.0
BRANCH=`git rev-parse --abbrev-ref HEAD`
COMMIT=`git rev-parse --short HEAD`
GO ?= go
GOTEST ?= $(GO) test
GOCOVER ?= $(GO) tool cover
COVEROUT ?= ./cover/c.out

vars:
	@echo DB_VERSION=${DB_VERSION}

.PHONY: test test_cover lint deps

test:
	@echo Starting to run tests locally with no coverage
	$(GOTEST) -race -count=1 -cover ./...
	@echo Done

test/cover:
	@echo Starting to run tests locally with coverage
	$(GOTEST) -cover -coverpkg=./... -coverprofile=$(COVEROUT) . && $(GOCOVER) -html=$(COVEROUT)
	@echo Done

lint:
	@echo Starting to run linter
	golangci-lint run ./...
	@echo Donego

deps:
	@echo Updating dependencies
	go mod tidy
	go mod vendor
