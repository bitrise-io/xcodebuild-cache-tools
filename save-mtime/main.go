package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// FileInfo represents information about a file
type FileInfo struct {
	Path        string    `json:"path"`
	Hash        string    `json:"hash"`
	ModTime     time.Time `json:"mod_time"`
	IsDirectory bool      `json:"is_directory"`
}

func hash_file(path string) (string, error) {
	// Open the file
	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("File open error: %v\n", err)
		return "", err
	}
	defer file.Close()

	// Calculate the SHA256 hash of the file
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	hashBytes := hash.Sum(nil)
	return hex.EncodeToString(hashBytes), nil
}

func main() {
	// Parse command-line arguments
	flag.Parse()
	rootDir := flag.Arg(0) // The root directory to start traversal

	// Verify root directory argument
	if rootDir == "" {
		fmt.Println("Usage: go run main.go /path/to/directory")
		os.Exit(1)
	}

	// Create a slice to store file info
	var fileInfos []FileInfo

	// Walk through the directory tree
	err := filepath.WalkDir(rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if d.IsDir() {
			return nil
		}

		inf, err := d.Info()
		if err != nil {
			return err
		}

		// Skip symbolic links
		if inf.Mode()&os.ModeSymlink != 0 {
			fmt.Printf("Skipping symbolic link: %s\n", path)
			return nil
		}

		// Calculate the SHA256 hash of the file
		hashString, err := hash_file(path)
		if err != nil {
			fmt.Printf("Error calculating hash: %v\n", err)
			return nil
		}

		relPath, err := filepath.Rel(rootDir, path)
		if err != nil {
			return err
		}

		// Create FileInfo object
		fileInfo := FileInfo{
			Path:        relPath,
			Hash:        hashString,
			ModTime:     inf.ModTime(),
			IsDirectory: inf.IsDir(),
		}

		// Append FileInfo to slice
		fileInfos = append(fileInfos, fileInfo)

		return nil
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	// Encode fileInfos slice to JSON
	jsonData, err := json.MarshalIndent(fileInfos, "", "    ")
	if err != nil {
		fmt.Printf("Error encoding JSON: %v\n", err)
		os.Exit(1)
	}

	// Write JSON data to a file
	outputFile := "file_info.json"
	err = os.WriteFile(outputFile, jsonData, 0644)
	if err != nil {
		fmt.Printf("Error writing JSON file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Processed %d files. File information saved to %s\n", len(fileInfos), outputFile)
}
