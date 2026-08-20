// Package s3journal stores a journal in an S3 bucket, and the leases
// that fence it. It uses the conditional writes of S3 for both.
package s3journal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/keel/keel/journal"
	"github.com/keel/keel/lease"
)

// maxEntrySize limits one entry object. A larger object is a defect in
// the caller, and a read of it must not exhaust memory.
const maxEntrySize = 8 << 20

// api is the part of the S3 client that the journal uses. A test
// supplies its own implementation.
type api interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// S3Journal keeps every lease and entry as an object in one bucket. It
// is safe for concurrent use.
type S3Journal struct {
	client     api
	bucket     string
	rootPrefix string
}

var (
	_ journal.Store = (*S3Journal)(nil)
	_ lease.Locker  = (*S3Journal)(nil)
)

// leaseRecord is the stored form of a lease. A released lease keeps its
// epoch, so a later holder never repeats an epoch.
type leaseRecord struct {
	Owner    string      `json:"owner"`
	Epoch    lease.Epoch `json:"epoch"`
	Expires  time.Time   `json:"expires"`
	Released bool        `json:"released,omitempty"`
}

// New returns a journal that uses the given client. Use it to supply a
// client with a custom endpoint or custom credentials.
func New(client api, bucket, rootPrefix string) (*S3Journal, error) {
	if client == nil {
		return nil, errors.New("s3journal: nil client")
	}
	if bucket == "" {
		return nil, errors.New("s3journal: empty bucket")
	}
	return &S3Journal{
		client:     client,
		bucket:     bucket,
		rootPrefix: strings.Trim(rootPrefix, "/"),
	}, nil
}

// NewS3Journal returns a journal that uses the default AWS configuration
// of the environment.
func NewS3Journal(bucket string, rootPrefix string) (*S3Journal, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("s3journal: load AWS config: %w", err)
	}
	return New(s3.NewFromConfig(cfg), bucket, rootPrefix)
}

func (j *S3Journal) leaseKey(invocationID string) string {
	return path.Join(j.rootPrefix, "invocations", invocationID, "lease.json")
}

func (j *S3Journal) entriesPrefix(invocationID string) string {
	return path.Join(j.rootPrefix, "invocations", invocationID, "entries") + "/"
}

// entryKey names an entry by its step. The step is zero-padded, so a
// listing returns the entries in the order that Read must give them.
func (j *S3Journal) entryKey(invocationID string, step int) string {
	return fmt.Sprintf("%s%020d.json", j.entriesPrefix(invocationID), step)
}

// entryRecord is the stored form of an entry. It keeps the epoch of the
// writer, so an operator can tell which holder recorded the step.
type entryRecord struct {
	journal.Entry
	Epoch lease.Epoch `json:"epoch"`
}

// Claim takes the lease for ttl. It returns lease.ErrClaimHeld if
// another owner holds a lease that is not expired.
func (j *S3Journal) Claim(ctx context.Context, resource, owner string, ttl time.Duration) (*lease.Lease, error) {
	if resource == "" || owner == "" {
		return nil, errors.New("s3journal: empty resource or owner")
	}
	if ttl <= 0 {
		return nil, errors.New("s3journal: ttl must be positive")
	}

	key := j.leaseKey(resource)
	current, etag, err := j.getLease(ctx, key)
	if err != nil {
		return nil, err
	}

	next := leaseRecord{Owner: owner, Epoch: 1, Expires: time.Now().Add(ttl)}
	if current != nil {
		if !current.Released && current.Owner != owner && time.Now().Before(current.Expires) {
			return nil, lease.ErrClaimHeld
		}
		next.Epoch = current.Epoch + 1
	}

	if err := j.putLease(ctx, key, next, etag); err != nil {
		// A failed condition means that another claimant won the race.
		if isPreconditionFailed(err) {
			return nil, lease.ErrClaimHeld
		}
		return nil, err
	}

	return lease.New(resource, owner, next.Epoch, next.Expires), nil
}

// Renew extends the lease under the same epoch. It returns
// lease.ErrLeaseLost if the stored lease is no longer this holder's.
func (j *S3Journal) Renew(ctx context.Context, l *lease.Lease, ttl time.Duration) error {
	if l == nil {
		return errors.New("s3journal: nil lease")
	}
	if ttl <= 0 {
		return errors.New("s3journal: ttl must be positive")
	}

	key := j.leaseKey(l.Resource)
	current, etag, err := j.getLease(ctx, key)
	if err != nil {
		return err
	}
	if !holds(current, l) {
		return lease.ErrLeaseLost
	}

	renewed := *current
	renewed.Expires = time.Now().Add(ttl)
	if err := j.putLease(ctx, key, renewed, etag); err != nil {
		// A failed condition means another holder wrote the lease first.
		if isPreconditionFailed(err) {
			return lease.ErrLeaseLost
		}
		return err
	}

	l.Extend(renewed.Expires)
	return nil
}

// live reports whether the stored record is an unexpired claim.
func live(current *leaseRecord) bool {
	if current == nil || current.Released {
		return false
	}
	return time.Now().Before(current.Expires)
}

// holds reports whether the stored record is still the lease's own, live
// claim. An expired record is not held, even by its own owner.
func holds(current *leaseRecord, l *lease.Lease) bool {
	return live(current) && current.Epoch == l.Epoch && current.Owner == l.Owner
}

// Release drops the lease. It is not an error to release a lease that
// the holder already lost.
func (j *S3Journal) Release(ctx context.Context, l *lease.Lease) error {
	if l == nil {
		return errors.New("s3journal: nil lease")
	}

	key := j.leaseKey(l.Resource)
	current, etag, err := j.getLease(ctx, key)
	if err != nil {
		return err
	}
	// Release ignores the expiry, so a late holder still tidies up its
	// own record and does not disturb a newer one.
	if current == nil || current.Released || current.Epoch != l.Epoch || current.Owner != l.Owner {
		return nil
	}

	released := *current
	released.Released = true
	released.Expires = time.Now()
	if err := j.putLease(ctx, key, released, etag); err != nil {
		// A failed condition means another engine claimed the lease
		// after the read, so this holder has nothing left to release.
		if isPreconditionFailed(err) {
			return nil
		}
		return err
	}
	return nil
}

// Append writes one entry under epoch. It returns lease.ErrLeaseLost if
// epoch is not the epoch of the live lease on the invocation.
func (j *S3Journal) Append(ctx context.Context, invocationID string, epoch lease.Epoch, e journal.Entry) error {
	if invocationID == "" {
		return errors.New("s3journal: empty invocation id")
	}
	if e.Step < 0 {
		return fmt.Errorf("s3journal: negative step %d", e.Step)
	}

	current, _, err := j.getLease(ctx, j.leaseKey(invocationID))
	if err != nil {
		return err
	}
	if !live(current) || current.Epoch != epoch {
		return lease.ErrLeaseLost
	}

	body, err := json.Marshal(entryRecord{Entry: e, Epoch: epoch})
	if err != nil {
		return fmt.Errorf("s3journal: marshal entry: %w", err)
	}

	_, err = j.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(j.bucket),
		Key:         aws.String(j.entryKey(invocationID, e.Step)),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
		// The step is written once. This condition is the fence that
		// stops a stale holder from forking the history.
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return j.classifyConflict(ctx, invocationID, e)
		}
		return fmt.Errorf("s3journal: put entry: %w", err)
	}
	return nil
}

// classifyConflict tells a repeat of a step from a different step that
// took its place. Only the first is safe for the caller to adopt.
func (j *S3Journal) classifyConflict(ctx context.Context, invocationID string, e journal.Entry) error {
	stored, err := j.getEntry(ctx, j.entryKey(invocationID, e.Step))
	if err != nil {
		return err
	}
	if stored.Name != e.Name {
		return fmt.Errorf("%w: step %d is %q in the journal and %q on replay",
			journal.ErrNonDeterministic, e.Step, stored.Name, e.Name)
	}
	return journal.ErrStepExists
}

// Read yields the entries of the invocation in step order. It needs no
// lease, and yields nothing for an unknown invocation.
func (j *S3Journal) Read(ctx context.Context, invocationID string) iter.Seq2[journal.Entry, error] {
	return func(yield func(journal.Entry, error) bool) {
		if invocationID == "" {
			yield(journal.Entry{}, errors.New("s3journal: empty invocation id"))
			return
		}

		pages := s3.NewListObjectsV2Paginator(j.client, &s3.ListObjectsV2Input{
			Bucket: aws.String(j.bucket),
			Prefix: aws.String(j.entriesPrefix(invocationID)),
		})

		for pages.HasMorePages() {
			page, err := pages.NextPage(ctx)
			if err != nil {
				yield(journal.Entry{}, fmt.Errorf("s3journal: list entries: %w", err))
				return
			}
			for _, obj := range page.Contents {
				e, err := j.getEntry(ctx, aws.ToString(obj.Key))
				if !yield(e, err) || err != nil {
					return
				}
			}
		}
	}
}

func (j *S3Journal) getEntry(ctx context.Context, key string) (journal.Entry, error) {
	out, err := j.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(j.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return journal.Entry{}, fmt.Errorf("s3journal: get entry %q: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(out.Body, maxEntrySize+1))
	if err != nil {
		return journal.Entry{}, fmt.Errorf("s3journal: read entry %q: %w", key, err)
	}
	if len(body) > maxEntrySize {
		return journal.Entry{}, fmt.Errorf("s3journal: entry %q is larger than %d bytes", key, maxEntrySize)
	}

	var rec entryRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		return journal.Entry{}, fmt.Errorf("s3journal: unmarshal entry %q: %w", key, err)
	}
	return rec.Entry, nil
}

// getLease returns the stored lease and its ETag. It returns a nil
// record if no lease object exists yet.
func (j *S3Journal) getLease(ctx context.Context, key string) (*leaseRecord, string, error) {
	out, err := j.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(j.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, "", nil
		}
		return nil, "", fmt.Errorf("s3journal: get lease: %w", err)
	}
	defer func() { _ = out.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(out.Body, maxEntrySize))
	if err != nil {
		return nil, "", fmt.Errorf("s3journal: read lease: %w", err)
	}

	var rec leaseRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		return nil, "", fmt.Errorf("s3journal: unmarshal lease: %w", err)
	}
	return &rec, aws.ToString(out.ETag), nil
}

// putLease writes the lease only if the stored object still has etag. An
// empty etag requires that no object exists.
func (j *S3Journal) putLease(ctx context.Context, key string, rec leaseRecord, etag string) error {
	body, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("s3journal: marshal lease: %w", err)
	}

	in := &s3.PutObjectInput{
		Bucket:      aws.String(j.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
	}
	if etag == "" {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = aws.String(etag)
	}

	if _, err := j.client.PutObject(ctx, in); err != nil {
		return fmt.Errorf("s3journal: put lease: %w", err)
	}
	return nil
}

func isNotFound(err error) bool {
	if _, ok := errors.AsType[*types.NoSuchKey](err); ok {
		return true
	}
	return hasErrorCode(err, "NoSuchKey", "NotFound")
}

func isPreconditionFailed(err error) bool {
	return hasErrorCode(err, "PreconditionFailed", "ConditionalRequestConflict")
}

func hasErrorCode(err error, codes ...string) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return slices.Contains(codes, apiErr.ErrorCode())
}
