package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/bitrise-io/go-utils/v2/log"
	"github.com/bitrise-io/go-utils/v2/retryhttp"
	"io"
	"net/http"
	"os"

	"github.com/bitrise-io/xcodebuild-cache-tools/ddcache-save/network"
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

func upload(filePath, appSlug, key, accessToken, baseUrl string, logger log.Logger) error {
	client := network.NewAPIClient(retryhttp.NewClient(logger), baseUrl, accessToken, logger)

	checksum, err := checksumOfFile(filePath)
	if err != nil {
		logger.Warnf(err.Error())
		// fail silently and continue
	}

	url := fmt.Sprintf("%s/cache/%s/%s", baseUrl, appSlug, key)

	buildCacheHeaders := map[string]string{
		"Authorization":                    fmt.Sprintf("Bearer %s", accessToken),
		"x-flare-blob-validation-sha256":   checksum,
		"x-flare-blob-validation-level":    "error",
		"x-flare-no-skip-duplicate-writes": "true",
	}
	err = client.UploadArchive(filePath, http.MethodPut, url, buildCacheHeaders)
	if err != nil {
		return fmt.Errorf("failed to upload archive: %w", err)
	}

	return nil
}

func uploadCacheArchive(filePath, branch, appSlug, accessToken, baseUrl string, logger log.Logger) error {
	return upload(filePath, appSlug, fmt.Sprintf("%s-archive", branch), accessToken, baseUrl, logger)
}

func uploadMetadata(filePath, branch, appSlug, accessToken, baseUrl string, logger log.Logger) error {
	return upload(filePath, appSlug, fmt.Sprintf("%s-metadata", branch), accessToken, baseUrl, logger)
}

func main() {
	logger := log.NewLogger()

	cacheArchive := flag.String("cache-archive", "", "Path to the cache archive file to upload")
	cacheMetadata := flag.String("cache-metadata", "", "Path to the metadata file to upload")
	uploadURL := flag.String("upload-url", "", "URL to upload the files to")
	token := flag.String("access-token", "", "Access-token")
	appSlug := flag.String("app-slug", "", "App slug")
	branch := flag.String("branch", "", "Branch")

	flag.Parse()

	if *cacheArchive == "" || *cacheMetadata == "" || *uploadURL == "" || *token == "" || *appSlug == "" || *branch == "" {
		fmt.Println("cache-archive, cache-metadata, token, app-slug, branch and upload-url are required")
		flag.Usage()
		os.Exit(1)
	}

	if err := uploadCacheArchive(*cacheArchive, *branch, *appSlug, *token, *uploadURL, logger); err != nil {
		fmt.Printf("Error uploading cache archive: %v\n", err)
		os.Exit(1)
	}

	if err := uploadMetadata(*cacheMetadata, *branch, *appSlug, *token, *uploadURL, logger); err != nil {
		fmt.Printf("Error uploading metadata: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Files uploaded successfully")
}
