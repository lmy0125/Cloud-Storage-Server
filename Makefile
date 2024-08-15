absPath = /Users/lmy/Documents/Class\ Assignment/CSE\ 224/proj5-lmy0125
.PHONY: install
install:
	rm -rf bin
	GOBIN=$(absPath)/bin go install ./...

.PHONY: run-blockstore
run-blockstore:
	go run cmd/SurfstoreServerExec/main.go -s block -p 8081 -l

.PHONY: run-block1
run-block1:
	go run cmd/SurfstoreClientExec/main.go -d -f example_config.txt ./tdata 1024

.PHONY: run-raft
run-raft:
	go run cmd/SurfstoreRaftServerExec/main.go -f example_config.txt -i $(IDX)

.PHONY: test
test:
	rm -rf ./test/_bin
	GOBIN=$(absPath)/test/_bin go get github.com/mattn/go-sqlite3
	GOBIN=$(absPath)/test/_bin go install ./...
	go test -v ./test/...

.PHONY: specific-test
specific-test:
	rm -rf ./test/_bin
	GOBIN=$(absPath)/test/_bin go get github.com/mattn/go-sqlite3
	GOBIN=$(absPath)/test/_bin go install ./...
	go test -v -run $(TEST_REGEX) -count=1 ./test/...

.PHONY: clean
clean:
	rm -rf bin/ test/_bin
