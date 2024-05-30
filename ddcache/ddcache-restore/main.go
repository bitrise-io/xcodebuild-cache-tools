package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"bytes"
	"github.com/bitrise-io/go-utils/v2/log"
	"github.com/bitrise-io/xcodebuild-cache-tools/ddcache/common/kv"
	"os/exec"
)

// ErrCacheNotFound ...
var ErrCacheNotFound = errors.New("no cache archive found for the provided keys")

func download(ctx context.Context, downloadPath, key, accessToken, cacheUrl string, decompress bool, logger log.Logger) error {
	logger.Infof("Downloading %s from %s\n", downloadPath, cacheUrl)
	logger.Infof("Get key: %s\n", key)
	buildCacheHost, insecureGRPC, err := kv.ParseUrlGRPC(cacheUrl)
	if err != nil {
		return fmt.Errorf(
			"the url grpc[s]://host:port format, %q is invalid: %w",
			cacheUrl, err,
		)
	}

	kvClient, err := kv.NewClient(ctx, kv.NewClientParams{
		UseInsecure: insecureGRPC,
		Host:        buildCacheHost,
		DialTimeout: 5 * time.Second,
		ClientName:  "kv",
		Token:       accessToken,
	}, logger)
	if err != nil {
		return fmt.Errorf("new kv client: %w", err)
	}

	kvReader, err := kvClient.StartGet(ctx, key, logger)
	if err != nil {
		return fmt.Errorf("create kv get client: %w", err)
	}
	defer kvReader.Close()

	if decompress {
		var outBuf bytes.Buffer

		c := exec.Command("sh", "-c", "stdbuf -i1M -o1M -e0 zstdcat | tar -xPpf -")
		c.Stdin = kvReader
		c.Stderr = &outBuf
		c.Stdout = &outBuf

		if err := c.Run(); err != nil {
			logger.Debugf("Tar output: %s\n", outBuf.String())
			return fmt.Errorf("failed to decompress archive: %w", err)
		}
		for _, f := range c.ExtraFiles {
			_ = f.Close()
		}
		logger.Debugf("Tar complete. Output: %s\n", outBuf.String())
	} else {
		file, err := os.Create(downloadPath)
		if err != nil {
			return fmt.Errorf("create %q: %w", downloadPath, err)
		}
		defer file.Close()

		if _, err := io.Copy(file, kvReader); err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.NotFound {
				return ErrCacheNotFound
			}
			logger.Errorf("Failed to download archive: %s", err)
			return fmt.Errorf("failed to download archive: %w", err)
		}
	}

	return nil
}

func main() {
	logger := log.NewLogger()
	logger.EnableDebugLog(true)

	cacheMetadataDownloadPath := flag.String("cache-metadata", "", "Download path for the cache metadata")
	serviceURL := flag.String("service-url", "", "Build Cache service URL")
	token := flag.String("access-token", "", "Access-token")
	branch := flag.String("branch", "", "Branch")

	flag.Parse()

	cacheArchiveKey := fmt.Sprintf("%s-archive-stream", *branch)
	cacheMetadataKey := fmt.Sprintf("%s-metadata-stream", *branch)

	if *cacheMetadataDownloadPath == "" || *serviceURL == "" || *token == "" || *branch == "" {
		fmt.Println("cache-archive, cache-metadata, access-token, branch and service-url are required")
		flag.Usage()
		os.Exit(1)
	}

	err := download(context.Background(),
		"", cacheArchiveKey, *token, *serviceURL,
		true, logger)
	if err != nil {
		logger.Errorf("Error downloading cache archive: %v\n", err)
		os.Exit(1)
	}

	err = download(context.Background(),
		*cacheMetadataDownloadPath, cacheMetadataKey, *token, *serviceURL,
		false, logger)
	if err != nil {
		logger.Errorf("Error downloading cache metadata: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Files downloaded successfully")
}
