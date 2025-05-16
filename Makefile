proto:
	protoc \
        --proto_path=proto \
        --go_out=proto \
        --go-grpc_out=proto \
        proto/*.proto
	@echo "Proto files generated in the 'proto' directory."

server:
	go build -o tmp/server ./server/...
	./tmp/server

client:
	go build -o tmp/client ./client/...
	./tmp/client -file Xcode_16.2.xip

client_compress:
	go build -o tmp/client ./client/...
	./tmp/client -file file.txt -d true

.PHONY: proto server client client_compress
