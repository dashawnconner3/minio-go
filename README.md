# MinIO Go Multipart Upload Fix

## Fix: Goroutine leak in multipart upload on context cancellation

### Problem
When a multipart upload is cancelled via context cancellation, upload part
goroutines can leak because the cancellation signal is not properly propagated
to all in-flight upload workers.

### Fix
Ensures that context cancellation is properly checked in the multipart upload
path, allowing all in-flight goroutines to clean up promptly.

### Test
```bash
go run main.go
```
