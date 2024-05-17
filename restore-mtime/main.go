package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// FileInfo represents information about a file
type FileInfo struct {
	Path    string `json:"path"`
	Hash    string `json:"hash"`
	ModTime string `json:"mod_time"`
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
	flag.Parse()
	rootDir := flag.Arg(0)          // The root directory to start traversal
	fileInfoJSONPath := flag.Arg(1) // The path to file_info.json

	// Verify root directory argument
	if rootDir == "" || fileInfoJSONPath == "" {
		fmt.Println("Usage: go run main.go /path/to/directory /path/to/file_info.json")
		os.Exit(1)
	}

	// Load file_info.json
	jsonData, err := os.ReadFile(fileInfoJSONPath)
	if err != nil {
		fmt.Printf("Error reading %s: %v\n", fileInfoJSONPath, err)
		os.Exit(1)
	}

	// Parse JSON data
	var fileInfos []FileInfo
	if err := json.Unmarshal(jsonData, &fileInfos); err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		os.Exit(1)
	}

	updated := 0

	// Update modification times
	for _, fileInfo := range fileInfos {
		// Construct full file path
		filePath := filepath.Join(rootDir, fileInfo.Path)

		// Skip if file doesn't exist
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			continue
		}

		h, err := hash_file(filePath)
		if err != nil {
			fmt.Printf("Error hashing file %s: %v\n", fileInfo.Path, err)
			continue
		}

		if h != fileInfo.Hash {
			continue
		}

		// Get current modification time
		modTime, err := time.Parse(time.RFC3339, fileInfo.ModTime)
		if err != nil {
			fmt.Printf("Error parsing modification time for %s: %v\n", fileInfo.Path, err)
			continue
		}

		// Set modification time
		if err := os.Chtimes(filePath, modTime, modTime); err != nil {
			fmt.Printf("Error setting modification time for %s: %v\n", fileInfo.Path, err)
		} else {
			updated++
		}
	}

	fmt.Printf("Parsed file infos: %d\n", len(fileInfos))
	fmt.Printf("Updated files: %d\n", updated)
}
