package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

func main() {
	fmt.Println("=== MinIO Multipart Upload Goroutine Leak Test ===")
	fmt.Println("")

	// Simulate a multipart upload where context gets cancelled
	var goroutinesBefore, goroutinesAfter int

	func() {
		goroutinesBefore = runtime.NumGoroutine()
		fmt.Printf("Goroutines before: %d\n", goroutinesBefore)

		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup

		// Simulate parallel upload parts
		for i := 0; i < 5; i++ {
			partID := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case <-time.After(100 * time.Millisecond):
					fmt.Printf("  Part %d uploaded\n", partID)
				case <-ctx.Done():
					fmt.Printf("  Part %d cancelled\n", partID)
				}
			}()
		}

		// Cancel after short delay (before uploads complete)
		time.Sleep(20 * time.Millisecond)
		fmt.Println("Cancelling context...")
		cancel()

		// Wait for all goroutines to exit
		wg.Wait()
	}()

	time.Sleep(50 * time.Millisecond)
	goroutinesAfter = runtime.NumGoroutine()
	fmt.Printf("Goroutines after: %d (before: %d)\n", goroutinesAfter, goroutinesBefore)

	if goroutinesAfter <= goroutinesBefore+2 {
		fmt.Println("\nOK: No goroutine leak detected - all upload goroutines cleaned up on cancel.")
	} else {
		fmt.Printf("\nWARNING: Possible goroutine leak! +%d goroutines\n",
			goroutinesAfter-goroutinesBefore)
	}
}
