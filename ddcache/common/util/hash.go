package util

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
)

func ChecksumOfFile(path string) (string, error) {
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
