// Package blobstore provides object storage abstractions for archiving raw HTML
// and WARC payloads to local disk or S3/MinIO compatible stores with gzip compression.
package blobstore

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var (
	// ErrObjectNotFound is returned when an object is not found in the store.
	ErrObjectNotFound = errors.New("object not found in store")
)

// Store represents an object storage backend (e.g. Local Filesystem or S3/MinIO).
type Store interface {
	Put(ctx context.Context, key string, data []byte, contentType string) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
}

// FileStore implements Store backed by the local filesystem with an S3-like directory structure.
type FileStore struct {
	baseDir string
}

// NewFileStore creates a local filesystem object store rooted at baseDir.
func NewFileStore(baseDir string) (*FileStore, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory %q: %w", baseDir, err)
	}
	return &FileStore{baseDir: baseDir}, nil
}

func (fs *FileStore) fullPath(key string) string {
	cleanKey := filepath.Clean(strings.TrimPrefix(key, "/"))
	return filepath.Join(fs.baseDir, cleanKey)
}

// Put writes data to the local filesystem at the path corresponding to key.
func (fs *FileStore) Put(ctx context.Context, key string, data []byte, contentType string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	target := fs.fullPath(key)
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory for key %q: %w", key, err)
	}

	tmpFile := fmt.Sprintf("%s.tmp.%d", target, os.Getpid())
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write payload tmp file: %w", err)
	}

	if err := os.Rename(tmpFile, target); err != nil {
		_ = os.Remove(tmpFile)
		return fmt.Errorf("failed to commit payload file: %w", err)
	}

	return nil
}

// Get reads data from the local filesystem for the specified key.
func (fs *FileStore) Get(ctx context.Context, key string) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	target := fs.fullPath(key)
	data, err := os.ReadFile(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}
	return data, nil
}

// Delete removes the file corresponding to key.
func (fs *FileStore) Delete(ctx context.Context, key string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	target := fs.fullPath(key)
	err := os.Remove(target)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Exists checks if an object exists.
func (fs *FileStore) Exists(ctx context.Context, key string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	target := fs.fullPath(key)
	info, err := os.Stat(target)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return !info.IsDir(), nil
}

// CompressGzip compresses byte data using gzip (BestCompression).
func CompressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecompressGzip uncompresses gzip-compressed byte data.
func DecompressGzip(compressed []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// FormatPayloadKey produces a standardized S3/object store key for a page payload.
func FormatPayloadKey(jobID, urlHash string) string {
	if jobID == "" {
		jobID = "standalone"
	}
	return fmt.Sprintf("payloads/%s/%s.html.gz", jobID, urlHash)
}
