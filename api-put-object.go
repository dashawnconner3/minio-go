package minio

import (
	"context"
	"io"
	"sync"
)

type partWork struct {
	Reader io.Reader
	PartNum int
	Err error
}

type partMetadata struct {
	Err error
	Part ObjectPart
}

type ObjectPart struct {
	PartNumber int
	ETag       string
	Size       int64
}

type CompletePart struct {
	PartNumber int
	ETag       string
}

type PutObjectOptions struct {
	UserMetadata map[string]string
}

func (c Client) uploadMultipartParts(ctx context.Context, bucket, object, uploadID string, reader io.Reader, size int64, opts PutObjectOptions) ([]CompletePart, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	optimalWorkers := 4
	queueCh := make(chan partWork, optimalWorkers)
	resultCh := make(chan partMetadata, optimalWorkers)

	var wg sync.WaitGroup
	for i := 0; i < optimalWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range queueCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				completedPart := ObjectPart{
					PartNumber: work.PartNum,
				}
				var err error

				select {
				case <-ctx.Done():
					return
				case resultCh <- partMetadata{
					Err:  err,
					Part: completedPart,
				}:
				}
			}
		}()
	}

	defer close(queueCh)

	go func() {
		for i := 1; i <= 10; i++ {
			select {
			case <-ctx.Done():
				return
			case queueCh <- partWork{PartNum: i}:
			}
		}
	}()

	var completedParts []CompletePart
	for i := 0; i < 10; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-resultCh:
			if res.Err != nil {
				return nil, res.Err
			}
			completedParts = append(completedParts, CompletePart{
				PartNumber: res.Part.PartNumber,
				ETag:       res.Part.ETag,
			})
		}
	}

	wg.Wait()
	return completedParts, nil
}
