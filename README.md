# Efficient Large Data Transfer with gRPC Streaming (Go Demo)

This project demonstrates an efficient method for transferring large files or data payloads between microservices using **gRPC server streaming** in Go. It serves as a gRPC-based alternative to traditional HTTP Range request solutions (like the one detailed in [file_upload/Readme.md](https://github.com/ekediala/file_upload) ) for handling large data transfers. It directly addresses the challenge posed by Sumit Mukhija in [this tweet](https://x.com/SumitM_X/status/1906687838609162530):

> "Your microservice needs to transfer large amounts of data (e.g., files, images) between services. How do you design the communication to avoid performance bottlenecks and manage large payloads efficiently?"

The core challenge, often faced in microservice architectures, is transferring significant amounts of data (files, images, datasets) without causing performance bottlenecks, excessive memory consumption, or network congestion. This implementation leverages gRPC's strengths to provide a robust and efficient solution.

## Why gRPC for Large File Transfers?

While HTTP Range requests offer a viable solution, gRPC provides several advantages in this context:

*   **Native Streaming:** gRPC has first-class support for server-side streaming, making it natural to send data in chunks without complex application-level logic for chunk management.
*   **Efficiency (HTTP/2):** gRPC typically runs over HTTP/2, benefiting from features like multiplexing and header compression, which can improve network efficiency compared to HTTP/1.1.
*   **Type Safety & Schema Definition:** Using Protocol Buffers (`.proto` files) enforces a clear contract between client and server, reducing integration errors.
*   **Performance:** The binary nature of Protocol Buffers and the efficiency of HTTP/2 often lead to better performance than text-based protocols like JSON over HTTP/1.1.

This project specifically showcases:

*   **Memory Efficiency**: Avoids loading large files into memory on either client or server using streaming and efficient buffer management.
*   **Resumable Downloads**: Client tracks progress and requests the next chunk from the correct offset.
*   **Performance**: Optimizes I/O using `io.CopyN` and pooled `bytes.Buffer`.

## Key Features (gRPC Implementation)

*   **gRPC Server Streaming**: The server streams file chunks to the client using a single RPC call (`StreamFile`).
*   **Protocol Buffers**: Defines the service contract (`TransferService`) and message formats (`StreamFileRequest`, `StreamFileResponse`, etc.) in `proto/transfer.proto`.
*   **Chunked Transfer**: Data is inherently sent in chunks managed by the server loop. Chunk size is configurable but capped on the server.
*   **Resumable Downloads**: The client requests the download starting from the last received byte offset (`Start` field in `StreamFileRequest`).
*   **Memory Efficiency**: Uses a `sync.Pool` of `bytes.Buffer` on the server combined with `io.CopyN` to efficiently read and buffer data without excessive allocations. The client uses `bufio.NewWriterSize` for efficient disk writes.
*   **Type Safety**: Leverages Go types generated from the `.proto` definition.
*   **Graceful Shutdown**: Client and server handle OS interrupt signals for clean termination.

## Project Structure

```
file_transfer/
├── Makefile           # Convenience script for running/building
├── go.mod             # Go module definition
├── go.sum             # Go module checksums
├── proto/
│   ├── generated/     # Generated Go code from .proto
│   │   └── transferpb/
│   │       └── transfer.pb.go
│   │       └── transfer_grpc.pb.go
│   └── transfer.proto # Protocol Buffers definition
├── client/
│   └── main.go        # Client service application
├── server/
│   └── main.go        # Server service application
└── files/             # Directory containing files served by the server (needs creation)
    └── (e.g., large_test_file.txt) # Sample file (you need to create this)
```

## How it Works

### Server (`server/main.go`)

*   Listens for gRPC connections on port 8000.
*   Serves files from the `./files` directory.
*   Uses a `sync.Pool` of `bytes.Buffer` for efficient memory use during chunk processing.
*   Implements the `TransferService`:
    *   **`GetFileSize` RPC:**
        *   Receives `GetFileSizeRequest` (containing `FileName`).
        *   Validates filename.
        *   Opens the file, gets stats (`os.Stat`).
        *   Returns `GetFileSizeResponse` with the `Size`.
        *   Closes the file handle (`defer file.Close()`).
    *   **`StreamFile` RPC (Server Streaming):**
        *   Receives `StreamFileRequest` (containing `FileName`, `Start` offset, `ChunkSize`).
        *   Performs security checks (no `..`, path cleaning, ensures path is within `files/`).
        *   Opens the requested file.
        *   Seeks to the `req.Start` offset in the file (`file.Seek`).
        *   Gets a `bytes.Buffer` from the `bufPool`.
        *   Sets the effective `chunkSize` (minimum of client request and server `MaxChunkSize`).
        *   Enters a loop:
            *   Uses `io.CopyN` to read exactly `chunkSize` bytes from the file into the `bytes.Buffer`.
            *   Handles `io.EOF`: If EOF is reached *and* the buffer contains data from a partial read, sends the remaining data before returning `io.EOF`. If EOF is reached with an empty buffer, returns `io.EOF` immediately.
            *   Sends the `buf.Bytes()` as a `StreamFileResponse` chunk using `stream.Send()`.
            *   If `stream.Send()` succeeds, calls `buf.Reset()` to clear the buffer for the next chunk.
        *   Returns `io.EOF` to the client upon successful completion (client interprets this as end-of-stream).
        *   Returns gRPC status errors for other issues.
        *   Ensures the `bytes.Buffer` is returned to the pool (`defer bufPool.Put(buf)`).

### Client (`client/main.go`)

*   Parses command-line arguments (`-file` flag for the filename).
*   Connects to the gRPC server (localhost:8000).
*   Sets up signal handling for graceful shutdown.
*   Opens the local destination file (`os.OpenFile` with `O_CREATE|O_RDWR`).
*   Gets local file stats (`file.Stat()`) to determine the current size (`info.Size()`). This is the starting point for resuming.
*   Calls the server's `GetFileSize` RPC to get the total expected size.
*   Checks if the local file size is already greater than or equal to the server's file size (already downloaded).
*   Calls the server's `StreamFile` RPC:
    *   Sends `StreamFileRequest` with:
        *   `FileName`: The requested file.
        *   `Start`: The current size of the local file (`info.Size()`), enabling resume.
        *   `ChunkSize`: Client's preferred chunk size (e.g., 512 bytes).
*   Initializes a `bufio.NewWriterSize` wrapping the local file handle for efficient disk writes.
*   Enters a receive loop (`for {}`):
    *   Calls `stream.Recv()` to get the next `StreamFileResponse` chunk.
    *   Checks for errors:
        *   If `err == io.EOF`, the server has finished sending. Break the loop.
        *   For other errors, log and exit.
    *   Writes the received `r.Chunk` to the `bufio.Writer` (`w.Write(r.Chunk)`).
    *   Checks for write errors.
*   After the loop (on EOF), flushes the `bufio.Writer` (implicitly handled by closing/defer, but explicit flush is safer if not deferring).
*   Logs total download time and size.

## Setup and Running

**Prerequisites:**

*   Go 1.21 or later installed.
*   Protocol Buffer Compiler (`protoc`). ([Installation Guide](https://grpc.io/docs/protoc-installation/))
*   Go plugins for protoc:
    ```bash
    go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
    ```
    Ensure your `GOPATH/bin` is in your system's `PATH`.

**Steps:**

1.  **Clone the repository (if you haven't already):**
    ```bash
    # Navigate to where you want the project
    git clone <your-repo-url> # Or use the existing directory if you have it
    cd file_transfer
    ```

2.  **Generate gRPC Code (if needed):**
    If the `proto/generated` directory is missing or out of date:
    ```bash
    make proto
    # Or manually:
    # protoc --go_out=./proto/generated --go_opt=paths=source_relative \
    #        --go-grpc_out=./proto/generated --go-grpc_opt=paths=source_relative \
    #        ./proto/transfer.proto
    ```

3.  **Tidy Go Modules:**
    ```bash
    go mod tidy
    ```

4.  **Create the `files` directory for the server:**
    ```bash
    mkdir files
    ```

5.  **Create a large test file:**
    This implementation is designed for large files. Create one inside the `files` directory (e.g., a 100 MiB file):
    ```bash
    # On Linux/macOS:
    fallocate -l 100M files/large_test_file.txt
    # Or use dd:
    # dd if=/dev/urandom of=files/large_test_file.txt bs=1M count=100
    ```
    *(Replace `large_test_file.txt` with your desired test filename)*

6.  **Build the Server and Client:**
    ```bash
    make build
    # Or manually:
    # go build -o ./tmp/server ./server/main.go
    # go build -o ./tmp/client ./client/main.go
    ```
    *(This assumes binaries are output to a `tmp` directory, adjust paths as needed)*

7.  **Start the Server:**
    Open a terminal:
    ```bash
    ./tmp/server
    ```
    You should see a log message like `{"level":"INFO","ts":...,"msg":"server running","port":8000,...}`

8.  **Run the Client:**
    Open *another* terminal:
    ```bash
    ./tmp/client -file large_test_file.txt
    ```
    *(Replace `large_test_file.txt` with the name of the file you created)*

9.  **Observe:**
    *   The server terminal will log incoming requests (if logging is added).
    *   The client terminal will log completion: `{"level":"INFO","ts":...,"msg":"file download complete","took":...,"size":...}`
    *   A file named `large_test_file.txt` (or your chosen name) will be created in the directory where you ran the client. Its size should match the original file in the `files` directory.
    *   Try interrupting the client (`Ctrl+C`) during download and re-running it. It should resume from where it left off.

10. **Stop the Server:**
    Press `Ctrl+C` in the server terminal. It should shut down gracefully.

## Configuration

Key parameters are defined as constants:

*   `server/main.go`:
    *   `Port`: 8000 (Server listening port)
    *   `ContentFolderName`: "files" (Directory to serve files from)
    *   `MaxChunkSize`: 128 (Server-side cap on chunk size in bytes)
    *   `BufferedReaderSize`: 64 (Internal buffer for `bufio` if used, though current impl uses `bytes.Buffer`)
*   `client/main.go`:
    *   `ServerPort`: 8000 (Port the client connects to)
    *   `ChunkSize`: 512 (Client's preferred chunk size in bytes)
    *   `BufferSize`: 64 * 1024 (64 KiB - `bufio.Writer` buffer size for disk writes)
    
## Memory Efficiency: Real-World Profiling

To validate the efficiency of this approach, we profiled the server's memory usage during the transfer of a 2GB+ file. The results were outstanding:

```
Showing nodes accounting for 512.05kB, 100% of 512.05kB total
      flat  flat%   sum%        cum   cum%
  512.05kB   100%   100%   512.05kB   100%  google.golang.org/grpc/internal/transport.(*writeQuota).get (inline)
         0     0%   100%   512.05kB   100%  ... (other stack frames)
```

**What does this mean?**

- **Extremely Low Memory Usage:**  
  Even when transferring a file over 2GB in size, the server's peak memory usage attributable to the transfer was only about 512KB. This confirms that the implementation streams data in small chunks and never loads the entire file into memory.

- **No Memory Leaks:**  
  The profile shows no evidence of memory leaks or excessive buffering. All allocations are related to gRPC's internal flow control, which is expected and minimal.

- **Scalability:**  
  Because memory usage does not scale with file size, this service can handle many concurrent large transfers without running into memory exhaustion.

**Conclusion:**  
This real-world profiling demonstrates that the gRPC streaming approach, combined with careful buffer management, achieves true memory efficiency for large file transfers—making it suitable for production environments where reliability and scalability are 

## Conclusion

This project demonstrates how gRPC server streaming, combined with efficient buffer management (`sync.Pool`, `bytes.Buffer`, `io.CopyN`), provides a powerful and performant alternative to HTTP-based methods for large file transfers between services. It offers type safety, potential performance benefits from HTTP/2, and a natural way to handle streaming data.