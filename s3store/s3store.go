// Package s3store keeps everything one invocation needs in one S3
// bucket: its record, its lease, and its journal. It uses the
// conditional writes of S3 for all three.
//
// One bucket means one failure domain and one prefix to delete. The
// interfaces stay separate, so another backend can implement any one of
// them alone.
package s3store

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

	"github.com/keel/keel/invocation"
	"github.com/keel/keel/journal"
	"github.com/keel/keel/lease"
)

// maxObjectSize limits one stored object. A larger object is a defect
// in the caller, and a read of it must not exhaust memory.
const maxObjectSize = 8 << 20

// api is the part of the S3 client that the journal uses. A test
// supplies its own implementation.
type api interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// Store keeps every record, lease, and entry as an object in one
// bucket. It is safe for concurrent use.
type Store struct {
	client     api
	bucket     string
	rootPrefix string
}

var (
	_ journal.Store           = (*Store)(nil)
	_ lease.Locker            = (*Store)(nil)
	_ invocation.Store        = (*Store)(nil)
	_ invocation.PendingIndex = (*Store)(nil)
)

// leaseRecord is the stored form of a lease. A released lease keeps its
// epoch, so a later holder never repeats an epoch.
type leaseRecord struct {
	Owner    string      `json:"owner"`
	Epoch    lease.Epoch `json:"epoch"`
	Expires  time.Time   `json:"expires"`
	Released bool        `json:"released,omitempty"`
}

// New returns a store that uses the given client. Use it to supply a
// client with a custom endpoint or custom credentials.
func New(client api, bucket, rootPrefix string) (*Store, error) {
	if client == nil {
		return nil, errors.New("s3store: nil client")
	}
	if bucket == "" {
		return nil, errors.New("s3store: empty bucket")
	}
	return &Store{
		client:     client,
		bucket:     bucket,
		rootPrefix: strings.Trim(rootPrefix, "/"),
	}, nil
}

// NewFromEnv returns a store that uses the default AWS configuration of
// the environment.
func NewFromEnv(bucket string, rootPrefix string) (*Store, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("s3store: load AWS config: %w", err)
	}
	return New(s3.NewFromConfig(cfg), bucket, rootPrefix)
}

// invocationPrefix is the subtree of one invocation. The key holds the
// service, the handler, and the id, which invocation.Validate checks.
func (j *Store) invocationPrefix(key string) string {
	return path.Join(j.rootPrefix, "invocations", key)
}

func (j *Store) recordKey(key string) string {
	return path.Join(j.invocationPrefix(key), "invocation.json")
}

func (j *Store) leaseKey(key string) string {
	return path.Join(j.invocationPrefix(key), "lease.json")
}

func (j *Store) entriesPrefix(key string) string {
	return path.Join(j.invocationPrefix(key), "entries") + "/"
}

// pendingPrefix is the index a dispatcher scans. A marker is an empty
// object, so a scan reads only the listing.
func (j *Store) pendingPrefix() string {
	return path.Join(j.rootPrefix, "pending") + "/"
}

func (j *Store) pendingKey(key string) string {
	return j.pendingPrefix() + key
}

// entryKey names an entry by its step. The step is zero-padded, so a
// listing returns the entries in the order that Read must give them.
func (j *Store) entryKey(invocationID string, step int) string {
	return fmt.Sprintf("%s%020d.json", j.entriesPrefix(invocationID), step)
}

// entryRecord is the stored form of an entry. The epoch is kept for
// attribution only, so an operator can tell which holder wrote the step.
type entryRecord struct {
	journal.Entry
	Epoch lease.Epoch `json:"epoch"`
}

// Create writes the record once and adds the invocation to the pending
// index. It returns invocation.ErrExists if the address is taken.
func (j *Store) Create(ctx context.Context, r invocation.Record) error {
	if err := r.Validate(); err != nil {
		return err
	}

	body, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("s3store: marshal record: %w", err)
	}

	key := r.Key()
	_, err = j.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(j.bucket),
		Key:         aws.String(j.recordKey(key)),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
		// The address is written once. A retried registration finds the
		// record instead of starting a second run.
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return invocation.ErrExists
		}
		return fmt.Errorf("s3store: put record: %w", err)
	}

	// The marker goes in after the record, so a scan never yields a key
	// that Get cannot read. A crash between the two leaves an
	// undispatched record, which is why Create is safe to repeat.
	if _, err := j.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(j.bucket),
		Key:    aws.String(j.pendingKey(key)),
		Body:   bytes.NewReader(nil),
	}); err != nil {
		return fmt.Errorf("s3store: put pending marker: %w", err)
	}
	return nil
}

// Get returns the record at key, and invocation.ErrNotFound if there is
// none.
func (j *Store) Get(ctx context.Context, key string) (invocation.Record, error) {
	if key == "" {
		return invocation.Record{}, fmt.Errorf("%w: empty key", invocation.ErrInvalid)
	}

	body, _, err := j.getObject(ctx, j.recordKey(key))
	if err != nil {
		if isNotFound(err) {
			return invocation.Record{}, invocation.ErrNotFound
		}
		return invocation.Record{}, fmt.Errorf("s3store: get record: %w", err)
	}

	var r invocation.Record
	if err := json.Unmarshal(body, &r); err != nil {
		return invocation.Record{}, fmt.Errorf("s3store: unmarshal record %q: %w", key, err)
	}
	return r, nil
}

// Pending yields the key of every invocation that is not dispatched.
func (j *Store) Pending(ctx context.Context) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		prefix := j.pendingPrefix()
		pages := s3.NewListObjectsV2Paginator(j.client, &s3.ListObjectsV2Input{
			Bucket: aws.String(j.bucket),
			Prefix: aws.String(prefix),
		})

		for pages.HasMorePages() {
			page, err := pages.NextPage(ctx)
			if err != nil {
				yield("", fmt.Errorf("s3store: list pending: %w", err))
				return
			}
			for _, obj := range page.Contents {
				key := strings.TrimPrefix(aws.ToString(obj.Key), prefix)
				if key == "" {
					continue
				}
				if !yield(key, nil) {
					return
				}
			}
		}
	}
}

// ClearPending drops key from the index. Clearing a key that is not in
// the index is not an error.
func (j *Store) ClearPending(ctx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("%w: empty key", invocation.ErrInvalid)
	}
	if _, err := j.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(j.bucket),
		Key:    aws.String(j.pendingKey(key)),
	}); err != nil {
		return fmt.Errorf("s3store: delete pending marker: %w", err)
	}
	return nil
}

// Claim takes the lease for ttl. It returns lease.ErrClaimHeld if
// another owner holds a lease that is not expired.
func (j *Store) Claim(ctx context.Context, resource, owner string, ttl time.Duration) (*lease.Lease, error) {
	if resource == "" || owner == "" {
		return nil, errors.New("s3store: empty resource or owner")
	}
	if ttl <= 0 {
		return nil, errors.New("s3store: ttl must be positive")
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
func (j *Store) Renew(ctx context.Context, l *lease.Lease, ttl time.Duration) error {
	if l == nil {
		return errors.New("s3store: nil lease")
	}
	if ttl <= 0 {
		return errors.New("s3store: ttl must be positive")
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
func (j *Store) Release(ctx context.Context, l *lease.Lease) error {
	if l == nil {
		return errors.New("s3store: nil lease")
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

// Append writes one entry. The conditional write on the entry key is
// the fence, so Append needs no lease and never reads one.
func (j *Store) Append(ctx context.Context, invocationID string, epoch lease.Epoch, e journal.Entry) error {
	if invocationID == "" {
		return errors.New("s3store: empty invocation id")
	}
	if e.Step < 0 {
		return fmt.Errorf("s3store: negative step %d", e.Step)
	}

	body, err := json.Marshal(entryRecord{Entry: e, Epoch: epoch})
	if err != nil {
		return fmt.Errorf("s3store: marshal entry: %w", err)
	}

	_, err = j.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(j.bucket),
		Key:         aws.String(j.entryKey(invocationID, e.Step)),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
		// The fence. This condition is atomic, so one writer wins the
		// step whatever the lease says, and the entry never changes.
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return j.classifyConflict(ctx, invocationID, e)
		}
		return fmt.Errorf("s3store: put entry: %w", err)
	}
	return nil
}

// classifyConflict tells a repeat of a step from a different step that
// took its place. Only the first is safe for the caller to adopt.
func (j *Store) classifyConflict(ctx context.Context, invocationID string, e journal.Entry) error {
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
func (j *Store) Read(ctx context.Context, invocationID string) iter.Seq2[journal.Entry, error] {
	return func(yield func(journal.Entry, error) bool) {
		if invocationID == "" {
			yield(journal.Entry{}, errors.New("s3store: empty invocation id"))
			return
		}

		pages := s3.NewListObjectsV2Paginator(j.client, &s3.ListObjectsV2Input{
			Bucket: aws.String(j.bucket),
			Prefix: aws.String(j.entriesPrefix(invocationID)),
		})

		for pages.HasMorePages() {
			page, err := pages.NextPage(ctx)
			if err != nil {
				yield(journal.Entry{}, fmt.Errorf("s3store: list entries: %w", err))
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

// getObject reads one object whole. It returns the raw error, so a
// caller can tell a missing object from a failure.
func (j *Store) getObject(ctx context.Context, key string) ([]byte, string, error) {
	out, err := j.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(j.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = out.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(out.Body, maxObjectSize+1))
	if err != nil {
		return nil, "", fmt.Errorf("s3store: read %q: %w", key, err)
	}
	if len(body) > maxObjectSize {
		return nil, "", fmt.Errorf("s3store: object %q is larger than %d bytes", key, maxObjectSize)
	}
	return body, aws.ToString(out.ETag), nil
}

func (j *Store) getEntry(ctx context.Context, key string) (journal.Entry, error) {
	body, _, err := j.getObject(ctx, key)
	if err != nil {
		return journal.Entry{}, fmt.Errorf("s3store: get entry %q: %w", key, err)
	}

	var rec entryRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		return journal.Entry{}, fmt.Errorf("s3store: unmarshal entry %q: %w", key, err)
	}
	return rec.Entry, nil
}

// getLease returns the stored lease and its ETag. It returns a nil
// record if no lease object exists yet.
func (j *Store) getLease(ctx context.Context, key string) (*leaseRecord, string, error) {
	body, etag, err := j.getObject(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return nil, "", nil
		}
		return nil, "", fmt.Errorf("s3store: get lease: %w", err)
	}

	var rec leaseRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		return nil, "", fmt.Errorf("s3store: unmarshal lease: %w", err)
	}
	return &rec, etag, nil
}

// putLease writes the lease only if the stored object still has etag. An
// empty etag requires that no object exists.
func (j *Store) putLease(ctx context.Context, key string, rec leaseRecord, etag string) error {
	body, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("s3store: marshal lease: %w", err)
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
		return fmt.Errorf("s3store: put lease: %w", err)
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
