DB_VERSION ?= 0.1
GO_VERSION ?= 1.16.0
BRANCH=`git rev-parse --abbrev-ref HEAD`
COMMIT=`git rev-parse --short HEAD`
GO ?= go
GOTEST := $(GO) test
GOTOOL := $(GO) tool
GOCOVER ?= $(GO) tool cover
COVEROUT ?= ./cover/c.out
TIMESTAMP := $$(date +%Y%m%d%H%M%S)

vars:
	@echo DB_VERSION=${DB_VERSION}
	@echo TIMESTAMP=${TIMESTAMP}

.PHONY: test test_cover lint deps pprof

test:
	@echo Starting to run tests locally with no coverage
	$(GOTEST) -race -count=1 -cover -v ./...
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

pprof: vars
	@echo Starting CPU profiling
	@echo Collecting Document JSON profile
	$(GOTEST) -bench=BenchmarkDocument_JSON -benchmem -run=xxx -cpuprofile ./pprof/document_json_cpu.pprof -memprofile ./pprof/document_json_mem.pprof -benchtime=10s > ./pprof/document_json_$(TIMESTAMP).bench
