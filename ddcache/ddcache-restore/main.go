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

	"github.com/bitrise-io/go-utils/v2/log"
	"github.com/bitrise-io/xcodebuild-cache-tools/ddcache/common/kv"
)

// ErrCacheNotFound ...
var ErrCacheNotFound = errors.New("no cache archive found for the provided keys")

func download(ctx context.Context, downloadPath, key, accessToken, cacheUrl string, logger log.Logger) error {
	logger.Infof("Downloading %s from %s\n", downloadPath, cacheUrl)
	buildCacheHost, insecureGRPC, err := kv.ParseUrlGRPC(cacheUrl)
	if err != nil {
		return fmt.Errorf(
			"the url grpc[s]://host:port format, %q is invalid: %w",
			cacheUrl, err,
		)
	}

	file, err := os.Create(downloadPath)
	if err != nil {
		return fmt.Errorf("create %q: %w", downloadPath, err)
	}
	defer file.Close()

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

	kvReader, err := kvClient.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("create kv get client: %w", err)
	}
	defer kvReader.Close()

	if _, err := io.Copy(file, kvReader); err != nil {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.NotFound {
			return ErrCacheNotFound
		}
		logger.Debugf("Failed to download archive: %s", err)
		return fmt.Errorf("failed to download archive: %w", err)
	}
	return nil
}

func main() {
	logger := log.NewLogger()
	logger.EnableDebugLog(true)

	cacheArchiveDownloadPath := flag.String("cache-archive", "", "Download path for the cache archive")
	cacheMetadataDownloadPath := flag.String("cache-metadata", "", "Download path for the cache metadata")
	serviceURL := flag.String("service-url", "", "Build Cache service URL")
	token := flag.String("access-token", "", "Access-token")
	branch := flag.String("branch", "", "Branch")

	flag.Parse()

	cacheArchiveKey := fmt.Sprintf("%s-archive", *branch)
	cacheMetadataKey := fmt.Sprintf("%s-metadata", *branch)

	if *cacheArchiveDownloadPath == "" || *cacheMetadataDownloadPath == "" || *serviceURL == "" || *token == "" || *branch == "" {
		fmt.Println("cache-archive, cache-metadata, access-token, branch and service-url are required")
		flag.Usage()
		os.Exit(1)
	}

	err := download(context.Background(), *cacheArchiveDownloadPath, cacheArchiveKey, *token, *serviceURL, logger)
	if err != nil {
		fmt.Printf("Error downloading cache archive: %v\n", err)
		os.Exit(1)
	}

	err = download(context.Background(), *cacheMetadataDownloadPath, cacheMetadataKey, *token, *serviceURL, logger)
	if err != nil {
		fmt.Printf("Error downloading cache metadata: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Files downloaded successfully")
}
