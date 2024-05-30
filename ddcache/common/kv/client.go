package kv

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bitrise-io/go-utils/v2/log"
	"github.com/bitrise-io/xcodebuild-cache-tools/ddcache/proto/kv_storage"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	bytestreamClient bytestream.ByteStreamClient
	bitriseKVClient  kv_storage.KVStorageClient
	logger           log.Logger
	clientName       string
	token            string
}

type NewClientParams struct {
	UseInsecure bool
	Host        string
	DialTimeout time.Duration
	ClientName  string
	Token       string
}

func NewClient(ctx context.Context, p NewClientParams, logger log.Logger) (*Client, error) {
	logger.Debugf("Creating client for %s", p.Host)
	ctx, cancel := context.WithTimeout(ctx, p.DialTimeout)
	defer cancel()
	creds := credentials.NewTLS(&tls.Config{})
	if p.UseInsecure {
		creds = insecure.NewCredentials()
	}
	transportOpt := grpc.WithTransportCredentials(creds)
	conn, err := grpc.DialContext(ctx, p.Host, transportOpt)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", p.Host, err)
	}

	return &Client{
		bytestreamClient: bytestream.NewByteStreamClient(conn),
		bitriseKVClient:  kv_storage.NewKVStorageClient(conn),
		clientName:       p.ClientName,
		token:            p.Token,
		logger:           logger,
	}, nil
}

type writer struct {
	logger       log.Logger
	stream       bytestream.ByteStream_WriteClient
	resourceName string
	offset       int64
	fileSize     int64
}

func (w *writer) Write(p []byte) (int, error) {
	req := &bytestream.WriteRequest{
		ResourceName: w.resourceName,
		WriteOffset:  w.offset,
		Data:         p,
	}
	if w.offset == 0 {
		w.logger.Debugf("Sending write request %d bytes", len(p))
	} else if len(p) < 1024*1024 || w.offset > 1024*1024 && ((w.offset/(1024*1024))%10 == 0) {
		w.logger.Debugf("Sending write request %d bytes @ offset %.2fMB", len(p), float32(w.offset)/(1024*1024))
	}
	err := w.stream.Send(req)
	switch {
	case errors.Is(err, io.EOF):
		return 0, io.EOF
	case err != nil:
		w.logger.Errorf("Error sending data: %v", err)
		return 0, fmt.Errorf("send data: %w", err)
	}
	w.offset += int64(len(p))
	return len(p), nil
}

func (w *writer) Close() error {
	w.logger.Debugf("sending finish write")
	err := w.stream.Send(&bytestream.WriteRequest{
		ResourceName: w.resourceName,
		WriteOffset:  w.offset,
		FinishWrite:  true,
	})
	if err != nil {
		w.logger.Errorf("Error sending finish write: %v", err)
		return fmt.Errorf("send finish write: %w", err)
	}

	w.logger.Debugf("Closing stream")
	_, err = w.stream.CloseAndRecv()
	if err != nil {
		w.logger.Errorf("Error sending finish write: %v", err)
		return fmt.Errorf("close stream: %w", err)
	}
	return nil
}

type reader struct {
	stream       bytestream.ByteStream_ReadClient
	buf          bytes.Buffer
	logger       log.Logger
	sumBytesRead int
}

func (r *reader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if r.sumBytesRead == 0 {
		r.logger.Debugf("Reading %d bytes", len(p))
	} else if len(p) < 1024*1024 || r.sumBytesRead > 1024*1024 && ((r.sumBytesRead/(1024*1024))%10 == 0) {
		r.logger.Debugf("Reading %d bytes. Sum read %.2fMB", len(p), float32(r.sumBytesRead)/(1024*1024))
	}

	bufLen := r.buf.Len()
	if bufLen > 0 {
		n, _ := r.buf.Read(p) // this will never fail
		r.sumBytesRead += n
		return n, nil
	}
	r.buf.Reset()

	resp, err := r.stream.Recv()
	switch {
	case errors.Is(err, io.EOF):
		return 0, io.EOF
	case err != nil:
		return 0, fmt.Errorf("stream receive: %w", err)
	}

	n := copy(p, resp.Data)
	r.sumBytesRead += n
	if n == len(resp.Data) {
		return n, nil
	}

	unwritenData := resp.Data[n:]
	_, _ = r.buf.Write(unwritenData) // this will never fail

	return n, nil
}

func (r *reader) Close() error {
	r.buf.Reset()
	return nil
}
