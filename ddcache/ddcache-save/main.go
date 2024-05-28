package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	humanize "github.com/dustin/go-humanize"

	"github.com/bitrise-io/go-utils/retry"
	"github.com/bitrise-io/go-utils/v2/log"
	"github.com/bitrise-io/xcodebuild-cache-tools/ddcache/common/kv"
	"github.com/bitrise-io/xcodebuild-cache-tools/ddcache/common/util"
)

func upload(filePath, key, accessToken, cacheUrl string, logger log.Logger) error {
	fmt.Printf("Initializing uploading %s to %s\n", filePath, cacheUrl)
	buildCacheHost, insecureGRPC, err := kv.ParseUrlGRPC(cacheUrl)
	if err != nil {
		return fmt.Errorf(
			"the url grpc[s]://host:port format, %q is invalid: %w",
			cacheUrl, err,
		)
	}

	checksum, err := util.ChecksumOfFile(filePath)
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

		fmt.Printf("Uploading %s - size %s\n", filePath, humanize.Bytes(uint64(stat.Size())))

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

func main() {
	logger := log.NewLogger()

	cacheArchive := flag.String("cache-archive", "", "Path to the cache archive file to upload")
	cacheMetadata := flag.String("cache-metadata", "", "Path to the metadata file to upload")
	uploadURL := flag.String("upload-url", "", "URL to upload the files to")
	accessToken := flag.String("access-token", "", "Access-token")
	branch := flag.String("branch", "", "Branch")

	flag.Parse()

	if *cacheArchive == "" || *cacheMetadata == "" || *uploadURL == "" || *accessToken == "" || *branch == "" {
		fmt.Println("cache-archive, cache-metadata, token, branch and upload-url are required")
		flag.Usage()
		os.Exit(1)
	}

	if err := upload(*cacheArchive, fmt.Sprintf("%s-archive", *branch), *accessToken, *uploadURL, logger); err != nil {
		fmt.Printf("Error uploading cache archive: %v\n", err)
		os.Exit(1)
	}

	if err := upload(*cacheMetadata, fmt.Sprintf("%s-metadata", *branch), *accessToken, *uploadURL, logger); err != nil {
		fmt.Printf("Error uploading metadata: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Files uploaded successfully")
}
