package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"time"

	"bytes"
	"errors"
	"github.com/bitrise-io/go-utils/retry"
	"github.com/bitrise-io/go-utils/v2/log"
	"github.com/bitrise-io/xcodebuild-cache-tools/ddcache/common/kv"
	"io"
	"sync"
)

func streamUploadFile(filePath string, fileFinished <-chan bool, writer io.WriteCloser, logger log.Logger) error {
	reader, err := os.OpenFile(filePath, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}

	buf := make([]byte, 1024*1024)

	done := false
	for !done {
		select {
		case <-fileFinished:
			logger.Debugf("File finished, setting done flag\n")
			done = true
		default:
		}

		readBytes, err := reader.Read(buf)

		if readBytes > 0 {
			_, err = writer.Write(buf[:readBytes])
			if err != nil {
				return fmt.Errorf("write to stream: %w", err)
			}
		}

		if readBytes == 0 || errors.Is(err, io.EOF) {
			if done {
				logger.Infof("File read completed\n")
				return nil
			} else {
				logger.Debugf("File read completed, but not done\n")
				// Let the compression stage write some bytes...
				time.Sleep(1 * time.Second)
			}
		} else if err != nil {
			return err
		}
	}

	return nil
}

func upload(path, key, accessToken, cacheUrl string, compress bool, logger log.Logger) error {
	logger.Infof("Initializing uploading %s to %s\n", path, cacheUrl)
	buildCacheHost, insecureGRPC, err := kv.ParseUrlGRPC(cacheUrl)
	if err != nil {
		return fmt.Errorf(
			"the url grpc[s]://host:port format, %q is invalid: %w",
			cacheUrl, err,
		)
	}

	logger.Infof("Put key: %s\n", key)
	const retries = 3
	err = retry.Times(retries).Wait(5 * time.Second).TryWithAbort(func(attempt uint) (error, bool) {
		if attempt != 0 {
			logger.Debugf("Retrying archive upload... (attempt %d)\n", attempt+1)
		}

		ctx := context.Background()
		kvClient, err := kv.NewClient(ctx, kv.NewClientParams{
			UseInsecure: insecureGRPC,
			Host:        buildCacheHost,
			DialTimeout: 5 * time.Second,
			ClientName:  "kv",
			Token:       accessToken,
		}, logger)
		if err != nil {
			return fmt.Errorf("new kv client: %w", err), false
		}

		kvWriter, err := kvClient.StartPut(ctx, kv.PutParams{
			Name: key,
			//Sha256Sum: checksum,
		})
		if err != nil {
			return fmt.Errorf("create kv put client: %w", err), false
		}

		if compress {
			tmpFile := os.TempDir() + "/ddcache-save-tmp" + key
			_ = os.Remove(tmpFile)

			_, err = os.OpenFile(tmpFile, os.O_RDONLY|os.O_CREATE, 0644)
			if err != nil {
				return fmt.Errorf("create tmp file: %w", err), true
			}

			c := exec.Command("sh", "-c", fmt.Sprintf("tar -cpPf - --format posix %s | stdbuf -i1M -o1M -e0 zstdcat > %s", path, tmpFile))
			var outputBuf bytes.Buffer
			c.Stderr = &outputBuf
			c.Stdout = &outputBuf

			tarFinishedChan := make(chan bool)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				err := streamUploadFile(tmpFile, tarFinishedChan, kvWriter, logger)
				if err != nil {
					logger.Errorf("Error uploading file: %v\n", err)
				}
				wg.Done()
			}()

			if err := c.Run(); err != nil {
				logger.Errorf("Error running tar. output: %s\n", outputBuf.String())
				return fmt.Errorf("run tar: %w", err), false
			}
			logger.Infof("Tar command completed.\n")
			logger.Debugf("Tar output: %s\n", outputBuf.String())
			close(tarFinishedChan)

			wg.Wait()
			logger.Debugf("Upload completed\n")
		} else {
			file, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0644)
			if err != nil {
				return fmt.Errorf("read file: %w", err), false
			}
			_, err = io.Copy(kvWriter, file)
			if err != nil {
				return fmt.Errorf("copy file: %w", err), false
			}
		}

		if err := kvWriter.Close(); err != nil {
			logger.Errorf("Error closing writer: %v\n", err)
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
	logger.EnableDebugLog(true)

	cacheDirectory := flag.String("cache-directory", "", "Path to the cache archive file to upload")
	cacheMetadata := flag.String("cache-metadata", "", "Path to the metadata file to upload")
	uploadURL := flag.String("upload-url", "", "URL to upload the files to")
	accessToken := flag.String("access-token", "", "Access-token")
	branch := flag.String("branch", "", "Branch")

	flag.Parse()

	if *cacheDirectory == "" || *cacheMetadata == "" || *uploadURL == "" || *accessToken == "" || *branch == "" {
		logger.Errorf("cache-directory, cache-metadata, token, branch and upload-url are required\n")
		flag.Usage()
		os.Exit(1)
	}

	if err := upload(*cacheDirectory,
		fmt.Sprintf("%s-archive-stream", *branch),
		*accessToken, *uploadURL,
		true,
		logger); err != nil {
		logger.Errorf("Error uploading cache archive: %v\n", err)
		os.Exit(1)
	}

	if err := upload(*cacheMetadata,
		fmt.Sprintf("%s-metadata-stream", *branch),
		*accessToken, *uploadURL,
		false,
		logger); err != nil {
		logger.Errorf("Error uploading metadata: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Files uploaded successfully")
}
