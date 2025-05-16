package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ekediala/file_transfer/proto/generated/transferpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	ServerPort = 8000
	ChunkSize  = 512
	BufferSize = 64 * 1024
)

func main() {
	var signals = []os.Signal{
		syscall.SIGINT,  // Ctrl+C
		syscall.SIGTERM, // Termination request
		syscall.SIGHUP,  // Terminal closed
	}
	ctx, cancel := signal.NotifyContext(context.Background(), signals...)
	defer cancel()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	logger = logger.With("app", "client")
	slog.SetDefault(logger)

	var fileName string
	var decompress bool
	flag.StringVar(&fileName, "file", "", "file to download")
	flag.BoolVar(&decompress, "d", false, "gzip decompression capability")
	flag.Parse()

	if fileName == "" {
		exit("invalid file name arg", errors.New("<usage> ./tmp/client -file <fileName>"))

	}

	if strings.Contains(fileName, "..") {
		exit("invalid file name", errors.New("invalid file name"))
	}

	start := time.Now()

	conn, err := grpc.NewClient(fmt.Sprintf(":%d", ServerPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		exit("grpc.NewClient()", err)
	}
	defer conn.Close()

	go func() {
		<-ctx.Done()
		if conn.GetState() != connectivity.Shutdown {
			conn.Close()
		}
	}()

	// os.O_APPEND allows us to append to the end of the file without
	// manually seeking to it
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		exit("os.OpenFile", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		exit("file.Stat", err)
	}

	fileSizeCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	client := transferpb.NewTransferClient(conn)
	res, err := client.GetFileSize(fileSizeCtx, &transferpb.GetFileSizeRequest{FileName: fileName})
	if err != nil {
		exit("client.GetFileSize()", err)
	}

	if info.Size() >= res.Size {
		slog.Info("file already downloaded")
		return
	}

	stream, err := client.StreamFile(ctx, &transferpb.StreamFileRequest{
		Start:         info.Size(),
		ChunkSize:     ChunkSize,
		FileName:      fileName,
		CanDecompress: decompress,
	})
	if err != nil {
		exit("client.StreamFile", err)
	}

	bytesDownloaded := info.Size()
	lastLogTime := time.Now()

	var r *transferpb.StreamFileResponse
	var streamErr error
	var chunkReader io.Reader

	for {
		r, streamErr = stream.Recv()
		if streamErr != nil {
			break
		}

		chunkReader = bytes.NewReader(r.Chunk)
		// if file is compressed, we decompress it first before readinf
		if r.Compressed {
			gr, err := gzip.NewReader(chunkReader)
			if err != nil {
				slog.Error("gzip decompression failed", "offset", bytesDownloaded, "err", err)
				exit("gzip.NewReader", err)
			}
			defer gr.Close()
			chunkReader = gr
		}

		_, streamErr = io.Copy(file, chunkReader)
		if streamErr != nil {
			break
		}

		bytesDownloaded += int64(len(r.Chunk))
		if time.Since(lastLogTime) > 3*time.Second {
			slog.Info("progress", "received", bytesDownloaded, "total", res.Size)
			lastLogTime = time.Now()
		}
	}

	if streamErr != nil && streamErr != io.EOF {
		if errors.Is(streamErr, context.Canceled) {
			slog.Warn("Download interrupted by signal")
			return
		}

		exit("Stream", streamErr)
	}

	downloadDuration := time.Since(start)
	bytesPerSecond := float64(bytesDownloaded) / downloadDuration.Seconds()
	slog.Info("download statistics",
		"size", fmt.Sprintf("%.2f MB", float64(bytesDownloaded)/1024/1024),
		"speed", fmt.Sprintf("%.2f MB/s", bytesPerSecond/1024/1024),
		"duration", fmt.Sprintf("%.2f seconds", downloadDuration.Seconds()),
	)
}

func exit(origin string, err error) {
	if err != nil {
		slog.Error(origin, "error", err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}
