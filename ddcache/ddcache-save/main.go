package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"time"

	humanize "github.com/dustin/go-humanize"

	"github.com/bitrise-io/go-utils/retry"
	"github.com/bitrise-io/go-utils/v2/log"
	"github.com/bitrise-io/xcodebuild-cache-tools/ddcache/common/kv"
)

func checksumOfFile(path string) (string, error) {
	hash := sha256.New()

	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close() //nolint:errcheck

	_, err = io.Copy(hash, file)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func parseUrlGRPC(s string) (string, bool, error) {
	parsed, err := url.ParseRequestURI(s)
	if err != nil {
		return "", false, fmt.Errorf("parse url: %w", err)
	}
	if parsed.Scheme != "grpc" && parsed.Scheme != "grpcs" {
		return "", false, fmt.Errorf("scheme must be grpc or grpcs")
	}
	if parsed.Port() == "" {
		return "", false, fmt.Errorf("must provide a port")
	}
	return parsed.Host, parsed.Scheme == "grpc", nil
}

func upload(filePath, key, accessToken, cacheUrl string, logger log.Logger) error {
	fmt.Printf("Uploading %s to %s\n", filePath, cacheUrl)
	buildCacheHost, insecureGRPC, err := parseUrlGRPC(cacheUrl)
	if err != nil {
		return fmt.Errorf(
			"the url grpc[s]://host:port format, %q is invalid: %w",
			cacheUrl, err,
		)
	}

	checksum, err := checksumOfFile(filePath)
	if err != nil {
		logger.Warnf(err.Error())
		// fail silently and continue
	}

	const retries = 3
	err = retry.Times(retries).Wait(5 * time.Second).TryWithAbort(func(attempt uint) (error, bool) {
		if attempt != 0 {
			logger.Debugf("Retrying archive upload... (attempt %d)", attempt+1)
		}

		ctx := context.Background()
		kvClient, err := kv.NewClient(ctx, kv.NewClientParams{
			UseInsecure: insecureGRPC,
			Host:        buildCacheHost,
			DialTimeout: 5 * time.Second,
			ClientName:  "kv",
			Token:       accessToken,
		})
		if err != nil {
			return fmt.Errorf("new kv client: %w", err), false
		}

		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("open %q: %w", filePath, err), false
		}
		defer file.Close()
		stat, err := file.Stat()
		if err != nil {
			return fmt.Errorf("stat %q: %w", filePath, err), false
		}

		fmt.Printf("Uploading %s to %s - size %s\n", filePath, cacheUrl, humanize.Bytes(uint64(stat.Size())))

		kvWriter, err := kvClient.Put(ctx, kv.PutParams{
			Name:      key,
			Sha256Sum: checksum,
			FileSize:  stat.Size(),
		})
		if err != nil {
			return fmt.Errorf("create kv put client: %w", err), false
		}
		if _, err := io.Copy(kvWriter, file); err != nil {
			return fmt.Errorf("upload archive: %w", err), false
		}
		if err := kvWriter.Close(); err != nil {
			return fmt.Errorf("close upload: %w", err), false
		}
		return nil, false
	})
	if err != nil {
		return fmt.Errorf("with retries: %w", err)
	}

	return nil
}

func uploadCacheArchive(filePath, branch, accessToken, cacheUrl string, logger log.Logger) error {
	return upload(filePath, fmt.Sprintf("%s-archive", branch), accessToken, cacheUrl, logger)
}

func uploadMetadata(filePath, branch, accessToken, cacheUrl string, logger log.Logger) error {
	return upload(filePath, fmt.Sprintf("%s-metadata", branch), accessToken, cacheUrl, logger)
}

func main() {
	logger := log.NewLogger()

	cacheArchive := flag.String("cache-archive", "", "Path to the cache archive file to upload")
	cacheMetadata := flag.String("cache-metadata", "", "Path to the metadata file to upload")
	uploadURL := flag.String("upload-url", "", "URL to upload the files to")
	token := flag.String("access-token", "", "Access-token")
	branch := flag.String("branch", "", "Branch")

	flag.Parse()

	if *cacheArchive == "" || *cacheMetadata == "" || *uploadURL == "" || *token == "" || *branch == "" {
		fmt.Println("cache-archive, cache-metadata, token, branch and upload-url are required")
		flag.Usage()
		os.Exit(1)
	}

	if err := uploadCacheArchive(*cacheArchive, *branch, *token, *uploadURL, logger); err != nil {
		fmt.Printf("Error uploading cache archive: %v\n", err)
		os.Exit(1)
	}

	if err := uploadMetadata(*cacheMetadata, *branch, *token, *uploadURL, logger); err != nil {
		fmt.Printf("Error uploading metadata: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Files uploaded successfully")
}
