package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"emperror.dev/errors"
	"github.com/cenkalti/backoff/v4"
	"github.com/juju/ratelimit"
	"github.com/mholt/archives"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/remote"
	"github.com/pterodactyl/wings/server/filesystem"
)

type S3Backup struct {
	Backup
}

var _ BackupInterface = (*S3Backup)(nil)

// Tune these as needed, or make them configurable!
var (
	DefaultTarThreads    = 6
	DefaultUploadThreads = 32
	SpeedLogInterval     = 5 * time.Second
)

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

// Streaming tar.gz archiver (run in a goroutine, writes to w)
func (s *S3Backup) streamTarGz(ctx context.Context, fsys *filesystem.Filesystem, ignore string, w io.Writer, tarThreads int) error {
	gz := gzip.NewWriter(w)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	rootPath := fsys.Path()

	// For simplicity, do NOT parallelize tar writing, as tar.Writer is not thread-safe.
	// If you want *aggressive* parallelism, it must be done by building the tar stream in order (hard).

	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(rootPath, path)
		if err != nil {
			return err
		}
		if ignore != "" && relPath == ignore {
			return nil
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = relPath
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			_, err = io.Copy(tw, f)
			f.Close()
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Generate creates a streaming backup and uploads it to S3 in parallel parts.
func (s *S3Backup) Generate(ctx context.Context, fsys *filesystem.Filesystem, ignore string) (*ArchiveDetails, error) {
	defer s.Remove()

	s.log().WithField("path", s.Path()).Info("starting streaming backup for server")

	// No reliable way to know size beforehand; use 0 for "unknown".
	size := int64(0)

	tarThreads := DefaultTarThreads
	uploadThreads := DefaultUploadThreads

	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		s.log().Info("tar.gz archiving started")
		err := s.streamTarGz(ctx, fsys, ignore, pw, tarThreads)
		if err != nil {
			s.log().WithError(err).Error("failed to stream archive to pipe")
			pw.CloseWithError(err)
		}
		s.log().Info("tar.gz archiving finished")
	}()

	urls, err := s.client.GetBackupRemoteUploadURLs(ctx, s.Uuid, size)
	if err != nil {
		return nil, err
	}
	s.log().WithField("parts", len(urls.Parts)).Info("got S3 upload urls from the Panel")

	parts, err := s.optimizedStreamToS3WithRetry(ctx, pr, size, urls, uploadThreads)
	if err != nil {
		return nil, err
	}
	ad, err := s.Details(ctx, parts)
	if err != nil {
		return nil, errors.WrapIf(err, "backup: failed to get archive details after upload")
	}
	return ad, nil
}

func (s *S3Backup) Restore(ctx context.Context, r io.Reader, callback RestoreCallback) error {
	reader := r
	// Respect write limit config (MB/s)
	if writeLimit := int64(config.Get().System.Backups.WriteLimit * 1024 * 1024); writeLimit > 0 {
		s.log().WithField("write_limit", writeLimit).Info("rate limiting restore")
		reader = ratelimit.Reader(r, ratelimit.NewBucketWithRate(float64(writeLimit), writeLimit))
	}
	// Updated: pass nil for the []string param, see your format.Extract docs.
	if err := format.Extract(ctx, reader, nil, func(ctx context.Context, f archives.FileInfo) error {
		fileReader, err := f.Open()
		if err != nil {
			s.log().WithField("name", f.NameInArchive).WithError(err).Error("failed to open archive file entry")
			return err
		}
		defer fileReader.Close()

		return callback(f.NameInArchive, f.FileInfo, fileReader)
	}); err != nil {
		s.log().WithError(err).Error("restore failed during extraction")
		return err
	}
	return nil
}

// Multi-threaded, speed-logged, retrying S3 multipart upload.
// Reads from pr, splits into parts, uploads in parallel.
func (s *S3Backup) optimizedStreamToS3WithRetry(ctx context.Context, pr io.Reader, size int64, urls *remote.BackupRemoteUploadURLs, uploadThreads int) ([]remote.BackupPart, error) {
	type uploadTask struct {
		Index int
		Part  string
		Size  int64
		Data  []byte
	}
	type uploadResult struct {
		Index int
		ETag  string
		Err   error
	}

	partCount := len(urls.Parts)
	partSize := urls.PartSize
	partsData := make([][]byte, partCount)
	var totalRead int64

	for i := 0; i < partCount; i++ {
		thisPartSize := partSize
		// For last part, read what's left (if known, else just try to fill)
		if i+1 == partCount && size > 0 {
			thisPartSize = size - int64(i)*partSize
		}
		buf := make([]byte, thisPartSize)
		n, err := io.ReadFull(pr, buf)
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			return nil, err
		}
		partsData[i] = buf[:n]
		atomic.AddInt64(&totalRead, int64(n))
		if n < int(thisPartSize) {
			break // EOF
		}
	}

	s.log().WithField("read", totalRead).Info("finished buffering all parts, beginning upload")

	tasks := make(chan uploadTask, partCount)
	results := make(chan uploadResult, partCount)
	var uploadedBytes int64

	uploadWorker := func() {
		for task := range tasks {
			s.log().WithField("part_id", task.Index+1).WithField("size", task.Size).Info("uploading backup part")
			etag, err := s.retryingUploadPart(ctx, task.Part, task.Data)
			atomic.AddInt64(&uploadedBytes, int64(len(task.Data)))
			results <- uploadResult{Index: task.Index, ETag: etag, Err: err}
		}
	}

	for i := 0; i < uploadThreads; i++ {
		go uploadWorker()
	}

	// Speed logger
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(SpeedLogInterval)
		defer ticker.Stop()
		start := time.Now()
		var lastBytes int64
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				curBytes := atomic.LoadInt64(&uploadedBytes)
				elapsed := time.Since(start).Seconds()
				avgSpeed := float64(curBytes) / elapsed
				intervalSpeed := float64(curBytes-lastBytes) / SpeedLogInterval.Seconds()
				s.log().
					WithField("uploaded", curBytes).
					WithField("average_speed", fmt.Sprintf("%.2f MB/s", avgSpeed/1024/1024)).
					WithField("interval_speed", fmt.Sprintf("%.2f MB/s", intervalSpeed/1024/1024)).
					Info("upload speed stats")
				lastBytes = curBytes
			}
		}
	}()

	// Feed tasks
	for i := 0; i < partCount; i++ {
		thisPartSize := int64(len(partsData[i]))
		tasks <- uploadTask{
			Index: i,
			Part:  urls.Parts[i],
			Size:  thisPartSize,
			Data:  partsData[i],
		}
	}
	close(tasks)

	uploadedParts := make([]remote.BackupPart, partCount)
	for i := 0; i < partCount; i++ {
		res := <-results
		if res.Err != nil {
			close(done)
			return nil, res.Err
		}
		uploadedParts[res.Index] = remote.BackupPart{
			ETag:       res.ETag,
			PartNumber: res.Index + 1,
		}
		s.log().WithField("part_id", res.Index+1).Info("successfully uploaded backup part")
	}
	close(done)
	s.log().WithField("parts", partCount).Info("backup has been successfully uploaded")
	return uploadedParts, nil
}

// Keeps retry/backoff logic
func (s *S3Backup) retryingUploadPart(ctx context.Context, part string, data []byte) (string, error) {
	return uploadPartWithBackoff(ctx, part, data, s.log())
}

// Upload part with exponential backoff and logging.
func uploadPartWithBackoff(ctx context.Context, part string, data []byte, logger interface{ WithField(string, interface{}) interface{ WithError(error) interface{ Warn(string) } } }) (string, error) {
	client := &http.Client{Timeout: time.Hour * 2}
	var etag string
	err := backoff.Retry(func() error {
		r, err := http.NewRequestWithContext(ctx, http.MethodPut, part, io.NopCloser(bytesReader(data)))
		if err != nil {
			return errors.Wrap(err, "backup: could not create request for S3")
		}
		r.ContentLength = int64(len(data))
		r.Header.Add("Content-Length", strconv.Itoa(len(data)))
		r.Header.Add("Content-Type", "application/x-gzip")
		res, err := client.Do(r)
		if err != nil {
			return errors.Wrap(err, "backup: S3 HTTP request failed")
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusOK {
			err := errors.New(fmt.Sprintf("backup: failed to put S3 object: [HTTP/%d] %s", res.StatusCode, res.Status))
			if res.StatusCode >= http.StatusInternalServerError {
				return err
			}
			return backoff.Permanent(err)
		}
		etag = res.Header.Get("ETag")
		return nil
	}, backoff.WithContext(newBackoff(), ctx))
	if err != nil {
		return "", err
	}
	return etag, nil
}

func newBackoff() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.Multiplier = 2
	b.MaxElapsedTime = time.Minute
	return b
}

// bytesReader returns an io.Reader for a []byte without extra allocations
func bytesReader(b []byte) io.Reader {
	return &byteReader{b: b}
}

type byteReader struct {
	b []byte
}

func (r *byteReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}

func (r *byteReader) Close() error { return nil }
