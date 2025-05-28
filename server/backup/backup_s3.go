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
	"github.com/juju/ratelimit"
	"github.com/mholt/archiver/v4"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/remote"
	"github.com/pterodactyl/wings/server/filesystem"
)

// S3Backup implements a backup stored in S3.
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

// Remove removes a backup from the system.
func (s *S3Backup) Remove() error {
	return os.Remove(s.Path())
}

// WithLogContext attaches additional context to the log output for this backup.
func (s *S3Backup) WithLogContext(c map[string]interface{}) {
	s.logContext = c
}

// Generate creates a new backup on the disk, moves it into the S3 bucket via
// the provided presigned URL, and then deletes the backup from the disk.
func (s *S3Backup) Generate(ctx context.Context, fsys *filesystem.Filesystem, ignore string) (*ArchiveDetails, error) {
	defer s.Remove()

	a := &filesystem.Archive{
		Filesystem: fsys,
		Ignore:     ignore,
	}

	s.log().WithField("path", s.Path()).Info("creating backup for server")
	if err := a.Create(ctx, s.Path()); err != nil {
		return nil, err
	}
	s.log().Info("created backup successfully")

	rc, err := os.Open(s.Path())
	if err != nil {
		return nil, errors.Wrap(err, "backup: could not read archive from disk")
	}
	defer rc.Close()

	parts, err := s.generateRemoteRequest(ctx, rc)
	if err != nil {
		return nil, err
	}
	ad, err := s.Details(ctx, parts)
	if err != nil {
		return nil, errors.WrapIf(err, "backup: failed to get archive details after upload")
	}
	return ad, nil
}

// Restore will read from the provided reader assuming that it is a gzipped
// tar reader. When a file is encountered in the archive the callback function
// will be triggered. If the callback returns an error the entire process is
// stopped, otherwise this function will run until all files have been written.
//
// This restoration uses a workerpool to use up to the number of CPUs available
// on the machine when writing files to the disk.
func (s *S3Backup) Restore(ctx context.Context, r io.Reader, callback RestoreCallback) error {
	reader := r
	// Steal the logic we use for making backups which will be applied when restoring
	// this specific backup. This allows us to prevent overloading the disk unintentionally.
	if writeLimit := int64(config.Get().System.Backups.WriteLimit * 1024 * 1024); writeLimit > 0 {
		reader = ratelimit.Reader(r, ratelimit.NewBucketWithRate(float64(writeLimit), writeLimit))
	}
	if err := format.Extract(ctx, reader, nil, func(ctx context.Context, f archiver.File) error {
		r, err := f.Open()
		if err != nil {
			return err
		}
		defer r.Close()
		return callback(f.NameInArchive, f.FileInfo, r)
	}); err != nil {
		return err
	}
	return nil
}

// Generates the remote S3 request and begins the upload.
// This version is parallel, memory-safe, and retries parts endlessly (unless context is canceled).
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

	uploader := newS3FileUploader(nil)
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

type s3FileUploader struct {
	client *http.Client
}

// newS3FileUploader returns a new file uploader instance.
func newS3FileUploader(_ io.Reader) *s3FileUploader {
	return &s3FileUploader{
		client: &http.Client{Timeout: time.Hour * 2},
	}
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

// Reader provides a wrapper around an existing io.Reader
// but implements io.Closer in order to satisfy an io.ReadCloser.
type Reader struct {
	io.Reader
}

func (Reader) Close() error {
	return nil
}
