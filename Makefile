build:
	@go build -o bin/velocitylog

run: build
	@./bin/velocitylog

run_test_locally:
	@go test ./... -v

run_test_in_container:
	@docker-compose up --build

clean:
	@rm -rf bin

gen_proto_types:
	chmod +x ./scripts/gen_types.sh