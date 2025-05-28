package backup

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "sync"
    "time"

    "emperror.dev/errors"
    "github.com/cenkalti/backoff/v4"
    "github.com/pterodactyl/wings/remote"
)

// --- S3Backup struct & constructor ---

type S3Backup struct {
    Backup
}

var _ BackupInterface = (*S3Backup)(nil)

func NewS3(client remote.Client, uuid string, ignore string) *S3Backup {
    return &S3Backup{
        Backup{
            client:  client,
            Uuid:    uuid,
            Ignore:  ignore,
            adapter: S3BackupAdapter,
        },
    }
}

// --- Parallelized, robust multipart upload logic ---

type s3FileUploader struct {
    client *http.Client
}

func newS3FileUploader(_ io.Reader) *s3FileUploader {
    return &s3FileUploader{
        client: &http.Client{Timeout: 2 * time.Hour},
    }
}

// The main logic: parallel, memory-efficient, endless retry per part
func (s *S3Backup) generateRemoteRequest(ctx context.Context, rc io.ReadCloser) ([]remote.BackupPart, error) {
    defer rc.Close()

    s.log().Debug("attempting to get size of backup...")
    size, err := s.Backup.Size()
    if err != nil {
        return nil, err
    }
    s.log().WithField("size", size).Debug("got size of backup")

    s.log().Debug("attempting to get S3 upload urls from Panel...")
    urls, err := s.client.GetBackupRemoteUploadURLs(context.Background(), s.Backup.Uuid, size)
    if err != nil {
        return nil, err
    }
    s.log().Debug("got S3 upload urls from the Panel")
    s.log().WithField("parts", len(urls.Parts)).Info("attempting to upload backup to s3 endpoint...")

    // Open the file for random access
    f, err := os.Open(s.Path())
    if err != nil {
        return nil, errors.Wrap(err, "backup: could not open archive from disk")
    }
    defer f.Close()

    type partResult struct {
        idx  int
        etag string
        err  error
    }

    uploader := newS3FileUploader(f) // only uses HTTP client

    numParts := len(urls.Parts)
    uploadedParts := make([]remote.BackupPart, numParts)
    results := make(chan partResult, numParts)

    // Limit number of concurrent uploads
    const maxParallel = 4
    sem := make(chan struct{}, maxParallel)
    var wg sync.WaitGroup

    for i, partURL := range urls.Parts {
        wg.Add(1)
        sem <- struct{}{} // Acquire a slot

        go func(idx int, partURL string) {
            defer wg.Done()
            defer func() { <-sem }() // Release slot

            offset := int64(idx) * urls.PartSize
            var partSize int64
            if idx+1 < numParts {
                partSize = urls.PartSize
            } else {
                partSize = size - (int64(idx) * urls.PartSize)
            }

            var etag string
            uploadFn := func() error {
                section := io.NewSectionReader(f, offset, partSize)
                reader := io.NopCloser(section)
                defer reader.Close()
                var err error
                etag, err = uploader.uploadPart(ctx, partURL, partSize, reader)
                if err != nil {
                    s.log().WithField("part_id", idx+1).WithError(err).Warn("failed to upload part, will retry")
                }
                return err
            }

            b := backoff.NewExponentialBackOff()
            b.MaxElapsedTime = 0 // never give up
            err := backoff.RetryNotify(uploadFn, backoff.WithContext(b, ctx),
                func(err error, d time.Duration) {
                    s.log().WithField("part_id", idx+1).WithError(err).
                        Warnf("retrying backup part after error (delay %s)", d)
                })

            if err == nil {
                s.log().WithField("part_id", idx+1).Info("successfully uploaded backup part")
            }

            results <- partResult{idx: idx, etag: etag, err: err}
        }(i, partURL)
    }

    go func() {
        wg.Wait()
        close(results)
    }()

    for res := range results {
        if res.err != nil {
            // Only possible if context canceled, otherwise retries forever
            return nil, fmt.Errorf("upload failed for part %d: %w", res.idx+1, res.err)
        }
        uploadedParts[res.idx] = remote.BackupPart{
            ETag:       res.etag,
            PartNumber: res.idx + 1,
        }
    }

    s.log().WithField("parts", numParts).Info("backup has been successfully uploaded")
    return uploadedParts, nil
}

// Accepts a reader for the part; used by each goroutine
func (fu *s3FileUploader) uploadPart(ctx context.Context, part string, size int64, reader io.ReadCloser) (string, error) {
    r, err := http.NewRequestWithContext(ctx, http.MethodPut, part, reader)
    if err != nil {
        return "", errors.Wrap(err, "backup: could not create request for S3")
    }
    r.ContentLength = size
    r.Header.Add("Content-Length", strconv.Itoa(int(size)))
    r.Header.Add("Content-Type", "application/x-gzip")

    res, err := fu.client.Do(r)
    if err != nil {
        if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
            return "", backoff.Permanent(err)
        }
        return "", errors.Wrap(err, "backup: S3 HTTP request failed")
    }
    defer res.Body.Close()

    if res.StatusCode != http.StatusOK {
        err := errors.New(fmt.Sprintf("backup: failed to put S3 object: [HTTP/%d] %s", res.StatusCode, res.Status))
        if res.StatusCode >= http.StatusInternalServerError {
            // Retryable
            return "", err
        }
        // Non-retryable
        return "", backoff.Permanent(err)
    }

    etag := res.Header.Get("ETag")
    return etag, nil
}
