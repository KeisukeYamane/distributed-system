compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go_opt=paths=source_relative \
		--proto_path=.

test:
	go test -race ./...

doc:
	godoc -http=localhost:6060