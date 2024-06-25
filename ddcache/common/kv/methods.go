package kv

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/bitrise-io/go-utils/v2/log"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/metadata"
)

type PutParams struct {
	Name      string
	Sha256Sum string
}

func (c *Client) StartPut(ctx context.Context, p PutParams) (io.WriteCloser, error) {
	md := metadata.Pairs(
		"authorization", fmt.Sprintf("bearer %s", c.token),
		"x-org-id", "dbd227a0aeb70859",
		"x-flare-blob-validation-sha256", p.Sha256Sum,
		"x-flare-blob-validation-level", "error",
		"x-flare-no-skip-duplicate-writes", "true",
	)
	ctx = metadata.NewOutgoingContext(ctx, md)
	stream, err := c.bitriseKVClient.Put(ctx)
	if err != nil {
		return nil, fmt.Errorf("initiate put: %w", err)
	}

	c.logger.Debugf("Stream initialized: %s", p.Name)
	resourceName := fmt.Sprintf("%s/%s", c.clientName, p.Name)

	return &writer{
		stream:       stream,
		resourceName: resourceName,
		offset:       0,
		logger:       c.logger,
	}, nil
}

func (c *Client) StartGet(ctx context.Context, name string, logger log.Logger) (io.ReadCloser, error) {
	resourceName := fmt.Sprintf("%s/%s", c.clientName, name)

	readReq := &bytestream.ReadRequest{
		ResourceName: resourceName,
		ReadOffset:   0,
		ReadLimit:    0,
	}
	md := metadata.Pairs(
		"authorization", fmt.Sprintf("Bearer %s", c.token),
		"x-org-id", "dbd227a0aeb70859")
	ctx = metadata.NewOutgoingContext(ctx, md)
	stream, err := c.bitriseKVClient.Get(ctx, readReq)
	if err != nil {
		return nil, fmt.Errorf("initiate get: %w", err)
	}

	c.logger.Debugf("Stream initialized: %s", name)

	return &reader{
		stream: stream,
		buf:    bytes.Buffer{},
		logger: logger,
	}, nil
}
