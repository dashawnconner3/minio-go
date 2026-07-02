package minio

import (
	"context"
	"bytes"
	"runtime"
	"testing"
	"time"
)

type Client struct{}

func TestMultipartUploadCancellationLeak(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	c := Client{}
	
	go func() {
		_, _ = c.uploadMultipartParts(ctx, "bucket", "object", "uploadID", bytes.NewReader(make([]byte, 1024*1024)), 1024*1024, PutObjectOptions{})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	time.Sleep(200 * time.Millisecond)
	postGoroutines := runtime.NumGoroutine()

	if postGoroutines > initialGoroutines+2 {
		t.Errorf("Goroutine leak detected: started with %d, ended with %d", initialGoroutines, postGoroutines)
	}
}
