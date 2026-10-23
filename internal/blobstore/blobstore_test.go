package blobstore

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestBlobStore_GzipRoundTrip(t *testing.T) {
	original := []byte("<html><head><title>Test Page</title></head><body>Hello Arachne World!</body></html>")

	compressed, err := CompressGzip(original)
	if err != nil {
		t.Fatalf("CompressGzip failed: %v", err)
	}

	if len(compressed) == 0 {
		t.Fatal("Compressed output is empty")
	}

	decompressed, err := DecompressGzip(compressed)
	if err != nil {
		t.Fatalf("DecompressGzip failed: %v", err)
	}

	if string(decompressed) != string(original) {
		t.Errorf("Decompressed %q, want %q", string(decompressed), string(original))
	}
}

func TestBlobStore_FileStoreOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "arachne-blobstore-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fs, err := NewFileStore(tempDir)
	if err != nil {
		t.Fatalf("NewFileStore failed: %v", err)
	}

	ctx := context.Background()
	key := FormatPayloadKey("job-123", "hash-abc")
	payload := []byte("compressed-payload-bytes")

	// 1. Exists before Put
	exists, err := fs.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists error: %v", err)
	}
	if exists {
		t.Error("Object should not exist before Put")
	}

	// 2. Put
	if err := fs.Put(ctx, key, payload, "application/gzip"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 3. Exists after Put
	exists, err = fs.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists error: %v", err)
	}
	if !exists {
		t.Error("Object should exist after Put")
	}

	// 4. Get
	retrieved, err := fs.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(retrieved) != string(payload) {
		t.Errorf("Get returned %q, want %q", string(retrieved), string(payload))
	}

	// 5. Delete
	if err := fs.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 6. Get after Delete returns ErrObjectNotFound
	_, err = fs.Get(ctx, key)
	if !errors.Is(err, ErrObjectNotFound) {
		t.Errorf("Get after delete expected ErrObjectNotFound, got %v", err)
	}
}

func TestBlobStore_FormatPayloadKey(t *testing.T) {
	key := FormatPayloadKey("job-uuid-1", "sha256hash")
	expected := "payloads/job-uuid-1/sha256hash.html.gz"
	if key != expected {
		t.Errorf("FormatPayloadKey = %q, want %q", key, expected)
	}

	keyFallback := FormatPayloadKey("", "sha256hash")
	expectedFallback := "payloads/standalone/sha256hash.html.gz"
	if keyFallback != expectedFallback {
		t.Errorf("FormatPayloadKey = %q, want %q", keyFallback, expectedFallback)
	}
}
