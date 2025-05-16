package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/ekediala/file_transfer/proto/generated/transferpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	Port              = 8000
	ContentFolderName = "files"
	MaxChunkSize      = 128 * 1024 // 128kb
)

type TransferService struct {
	transferpb.UnimplementedTransferServer
}

var bufPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

func (t *TransferService) StreamFile(req *transferpb.StreamFileRequest, stream grpc.ServerStreamingServer[transferpb.StreamFileResponse]) error {
	if strings.Contains(req.FileName, "..") {
		return status.Error(codes.InvalidArgument, "Invalid file name")
	}

	dir, err := os.Getwd()
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	fileName := path.Join(dir, ContentFolderName, req.FileName)
	fileName = filepath.Clean(fileName)
	// Ensure the path is still within the content directory
	contentDir := path.Join(dir, ContentFolderName)
	if !strings.HasPrefix(fileName, contentDir) {
		return status.Error(codes.InvalidArgument, "Invalid file path")
	}

	file, err := os.Open(fileName)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	if info.Size() <= req.Start {
		return nil
	}

	_, err = file.Seek(req.Start, io.SeekStart)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	buf, _ := bufPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		bufPool.Put(buf)
	}()

	chunkSize := min(req.ChunkSize, MaxChunkSize)
	buf.Grow(int(chunkSize))

	contentType := getContentType(fileName, file)
	canDeCompress := req.CanDecompress
	shouldCompress := canDeCompress && isCompressible(contentType)

	sendResponse := func(compressionStatus bool) error {
		return stream.Send(&transferpb.StreamFileResponse{Chunk: buf.Bytes(), Compressed: compressionStatus})
	}

	copyData := func(w io.Writer) (int64, error) {
		n, err := io.CopyN(w, file, int64(chunkSize))
		if err != nil && n <= 0 {
			if err == io.EOF {
				return n, io.EOF
			}
			return n, status.Error(codes.Internal, err.Error())
		}
		return n, nil
	}

	var w *gzip.Writer
	if shouldCompress {
		w = gzip.NewWriter(buf)
	}

	for {
		if shouldCompress {
			w.Reset(buf) // reset gzipWriter
			n, err := copyData(w)
			if err != nil && n <= 0 {
				if err == io.EOF {
					return nil
				}
				return status.Error(codes.Internal, err.Error())
			}

			err = w.Close() // flush contents to buf
			if err != nil {
				return status.Error(codes.Internal, err.Error())
			}

			err = sendResponse(true)
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return status.Error(codes.Internal, err.Error())
			}

		} else {
			n, err := copyData(buf)
			if err != nil && n <= 0 {
				if err == io.EOF {
					return nil
				}
				return status.Error(codes.Internal, err.Error())
			}

			err = sendResponse(false)
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return status.Error(codes.Internal, err.Error())
			}
		}

		buf.Reset()
	}
}

func (t *TransferService) GetFileSize(ctx context.Context, req *transferpb.GetFileSizeRequest) (*transferpb.GetFileSizeResponse, error) {
	if strings.Contains(req.FileName, "..") {
		return nil, status.Error(codes.InvalidArgument, "Invalid file name")
	}

	dir, err := os.Getwd()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	fileName := path.Join(dir, ContentFolderName, req.FileName)
	file, err := os.Open(fileName)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &transferpb.GetFileSizeResponse{Size: info.Size()}, nil
}

func main() {
	var signals = []os.Signal{
		syscall.SIGINT,  // Ctrl+C
		syscall.SIGTERM, // Termination request
		syscall.SIGHUP,  // Terminal closed
	}
	ctx, cancel := signal.NotifyContext(context.Background(), signals...)
	defer cancel()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	logger = logger.With("app", "server")
	slog.SetDefault(logger)

	cfg := &net.ListenConfig{}
	l, err := cfg.Listen(ctx, "tcp", fmt.Sprintf(":%d", Port))
	if err != nil {
		exit("cfg.Listen()", err)
	}

	server := grpc.NewServer()
	srv := &TransferService{}
	transferpb.RegisterTransferServer(server, srv)

	go func() {
		<-ctx.Done()
		server.Stop()
		exit("", nil)
	}()

	go func() {
		slog.Info("pprof", "error", http.ListenAndServe("localhost:6060", nil))
	}()

	slog.Info("server running", "port", Port, "numCPU", runtime.NumCPU())
	if err := server.Serve(l); err != nil {
		exit("server.Serve()", err)
	}
}

func exit(origin string, err error) {
	if err != nil {
		slog.Error(origin, "error", err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}

func isCompressible(contentType string) bool {
	compressibleTypes := []string{
		"text/", "application/json", "application/xml",
		"application/javascript", "application/x-javascript",
	}

	for _, t := range compressibleTypes {
		if strings.Contains(contentType, t) {
			return true
		}
	}
	return false
}

// getContentType determines the content type of a file using both
// extension-based matching and content sniffing when necessary.
func getContentType(fileName string, fileReader io.ReadSeeker) string {
	// 1. Try to determine content type from file extension
	ext := strings.ToLower(path.Ext(fileName))
	if ext != "" {
		mimeType := mime.TypeByExtension(ext)
		if mimeType != "" {
			if idx := strings.Index(mimeType, ";"); idx != -1 {
				return mimeType[:idx]
			}
			return mimeType
		}
	}

	// 2. For common extensions not in standard library
	switch ext {
	case ".md":
		return "text/markdown"
	case ".jsx", ".tsx":
		return "application/javascript"
	case ".yaml", ".yml":
		return "application/yaml"
		// ... other custom mappings
	}

	// 3. If we have a file reader, try content sniffing
	if fileReader != nil {
		// Save current position
		currentPos, err := fileReader.Seek(0, io.SeekCurrent)
		if err == nil {
			// Read first 512 bytes for content detection
			buffer := make([]byte, 512)
			n, err := fileReader.Read(buffer)

			// Restore original position
			fileReader.Seek(currentPos, io.SeekStart)

			if err == nil {
				// Detect content type
				return http.DetectContentType(buffer[:n])
			}
		}
	}

	// 4. Fallback to binary
	return "application/octet-stream"
}
