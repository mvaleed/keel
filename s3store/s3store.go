// Package s3store keeps everything one invocation needs in one S3
// bucket: its record, its lease, and its journal. It uses the
// conditional writes of S3 for all three.
//
// One bucket means one failure domain and one prefix to delete. The
// interfaces stay separate, so another backend can implement any one of
// them alone.
//
// Two kinds of key meet in this package. A key addresses one invocation,
// such as "demo/Charge/id-1", and every exported method takes one. An
// object key names one S3 object, and only this package builds or reads
// one. The unexported ...Key and ...Prefix helpers turn the first into
// the second, and they are the only place the two ever mix.
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
	"strconv"
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

// api is the part of the S3 client that the store uses. A test
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
	_ journal.Store       = (*Store)(nil)
	_ lease.Locker        = (*Store)(nil)
	_ invocation.Store    = (*Store)(nil)
	_ invocation.DueIndex = (*Store)(nil)
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

// invocationPrefix returns the object prefix of one invocation. The key
// holds the service, the handler, and the id, which Validate checks.
func (s *Store) invocationPrefix(key string) string {
	return path.Join(s.rootPrefix, "invocations", key)
}

func (s *Store) recordKey(key string) string {
	return path.Join(s.invocationPrefix(key), "invocation.json")
}

func (s *Store) leaseKey(key string) string {
	return path.Join(s.invocationPrefix(key), "lease.json")
}

func (s *Store) entriesPrefix(key string) string {
	return path.Join(s.invocationPrefix(key), "entries") + "/"
}

// duePrefix is the index a dispatcher scans. A marker is an empty
// object, so a scan reads only the listing.
func (s *Store) duePrefix() string {
	return path.Join(s.rootPrefix, "due") + "/"
}

// dueDigits pads the due seconds, so that lexicographic order over the
// listing is numeric order over the time.
const dueDigits = 20

// markerKey names a marker by its due time and then its invocation. A
// due time before the epoch is not representable and is clamped to it.
func (s *Store) markerKey(key string, due time.Time) string {
	secs := due.Unix()
	if secs < 0 {
		secs = 0
	}
	return fmt.Sprintf("%s%0*d/%s", s.duePrefix(), dueDigits, secs, key)
}

// parseMarker is the inverse of markerKey. invocation.Validate rejects a
// separator in every part, so the segments after the due parse back.
func (s *Store) parseMarker(objectKey string) (invocation.WakeupMarker, error) {
	rest := strings.TrimPrefix(objectKey, s.duePrefix())
	due, key, ok := strings.Cut(rest, "/")
	if !ok || key == "" {
		return invocation.WakeupMarker{}, fmt.Errorf("s3store: malformed marker %q", objectKey)
	}
	secs, err := strconv.ParseInt(due, 10, 64)
	if err != nil {
		return invocation.WakeupMarker{}, fmt.Errorf("s3store: malformed marker %q: %w", objectKey, err)
	}
	return invocation.WakeupMarker{Key: key, Due: time.Unix(secs, 0).UTC()}, nil
}

// entryKey names an entry by its step. The step is zero-padded, so a
// listing returns the entries in the order that Read must give them.
func (s *Store) entryKey(key string, step int) string {
	return fmt.Sprintf("%s%020d.json", s.entriesPrefix(key), step)
}

// entryRecord is the stored form of an entry. The epoch is kept for
// attribution only, so an operator can tell which holder wrote the step.
type entryRecord struct {
	journal.Entry
	Epoch lease.Epoch `json:"epoch"`
}

// Create writes the record once and schedules it to run at once. It
// returns invocation.ErrExists if the address is taken.
func (s *Store) Create(ctx context.Context, r invocation.Record) error {
	if err := r.Validate(); err != nil {
		return err
	}

	body, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("s3store: marshal record: %w", err)
	}

	key := r.Key()
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(s.recordKey(key)),
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
	// that Get cannot read. A crash between the two hides the record
	// from every scan, which is an accepted limit of this design.
	return s.Schedule(ctx, key, r.CreatedAt)
}

// Get returns the record at key, and invocation.ErrNotFound if there is
// none.
func (s *Store) Get(ctx context.Context, key string) (invocation.Record, error) {
	r, _, err := s.getRecord(ctx, key)
	return r, err
}

// getRecord returns the record and its ETag, which Update needs for the
// conditional write.
func (s *Store) getRecord(ctx context.Context, key string) (invocation.Record, string, error) {
	if key == "" {
		return invocation.Record{}, "", fmt.Errorf("%w: empty key", invocation.ErrInvalid)
	}

	body, etag, err := s.getObject(ctx, s.recordKey(key))
	if err != nil {
		if isNotFound(err) {
			return invocation.Record{}, "", invocation.ErrNotFound
		}
		return invocation.Record{}, "", fmt.Errorf("s3store: get record: %w", err)
	}

	var r invocation.Record
	if err := json.Unmarshal(body, &r); err != nil {
		return invocation.Record{}, "", fmt.Errorf("s3store: unmarshal record %q: %w", key, err)
	}
	return r, etag, nil
}

// Update replaces the record. It returns lease.ErrLeaseLost when the
// stored record carries a later epoch, because a stale holder wrote it.
func (s *Store) Update(ctx context.Context, r invocation.Record) error {
	if err := r.Validate(); err != nil {
		return err
	}

	key := r.Key()
	stored, etag, err := s.getRecord(ctx, key)
	if err != nil {
		return err
	}
	if stored.Epoch > r.Epoch {
		return lease.ErrLeaseLost
	}

	body, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("s3store: marshal record: %w", err)
	}

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(s.recordKey(key)),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
		// The epoch check and the write are separate, so the ETag closes
		// the gap in which another holder wrote the record.
		IfMatch: aws.String(etag),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return lease.ErrLeaseLost
		}
		return fmt.Errorf("s3store: put record: %w", err)
	}
	return nil
}

// Schedule writes a marker that falls due at the given time. The marker
// body is empty, because a listing returns the keys and not the bodies.
func (s *Store) Schedule(ctx context.Context, key string, due time.Time) error {
	if key == "" {
		return fmt.Errorf("%w: empty key", invocation.ErrInvalid)
	}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.markerKey(key, due)),
		Body:   bytes.NewReader(nil),
	}); err != nil {
		return fmt.Errorf("s3store: put due marker: %w", err)
	}
	return nil
}

// Due yields every marker due at or before now, earliest first. The due
// time leads the key, so the scan stops at the first future marker and
// its cost follows the ready work and not the backlog.
func (s *Store) Due(ctx context.Context, now time.Time) iter.Seq2[invocation.WakeupMarker, error] {
	return func(yield func(invocation.WakeupMarker, error) bool) {
		pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(s.duePrefix()),
		})

		for pages.HasMorePages() {
			page, err := pages.NextPage(ctx)
			if err != nil {
				yield(invocation.WakeupMarker{}, fmt.Errorf("s3store: list due: %w", err))
				return
			}
			for _, obj := range page.Contents {
				m, err := s.parseMarker(aws.ToString(obj.Key))
				if err != nil {
					if !yield(invocation.WakeupMarker{}, err) {
						return
					}
					continue
				}
				if m.Due.After(now) {
					return
				}
				if !yield(m, nil) {
					return
				}
			}
		}
	}
}

// Forget drops the exact marker, and not every marker that names its
// invocation. A repeated Schedule can leave a second one.
func (s *Store) Forget(ctx context.Context, m invocation.WakeupMarker) error {
	if m.Key == "" {
		return fmt.Errorf("%w: empty key", invocation.ErrInvalid)
	}
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.markerKey(m.Key, m.Due)),
	}); err != nil {
		return fmt.Errorf("s3store: delete due marker: %w", err)
	}
	return nil
}

// Claim takes the lease for ttl. It returns lease.ErrClaimHeld if
// another owner holds a lease that is not expired.
func (s *Store) Claim(ctx context.Context, resource, owner string, ttl time.Duration) (*lease.Lease, error) {
	if resource == "" || owner == "" {
		return nil, errors.New("s3store: empty resource or owner")
	}
	if ttl <= 0 {
		return nil, errors.New("s3store: ttl must be positive")
	}

	objectKey := s.leaseKey(resource)
	current, etag, err := s.getLease(ctx, objectKey)
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

	if err := s.putLease(ctx, objectKey, next, etag); err != nil {
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
func (s *Store) Renew(ctx context.Context, l *lease.Lease, ttl time.Duration) error {
	if l == nil {
		return errors.New("s3store: nil lease")
	}
	if ttl <= 0 {
		return errors.New("s3store: ttl must be positive")
	}

	objectKey := s.leaseKey(l.Resource)
	current, etag, err := s.getLease(ctx, objectKey)
	if err != nil {
		return err
	}
	if !holds(current, l) {
		return lease.ErrLeaseLost
	}

	renewed := *current
	renewed.Expires = time.Now().Add(ttl)
	if err := s.putLease(ctx, objectKey, renewed, etag); err != nil {
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
func (s *Store) Release(ctx context.Context, l *lease.Lease) error {
	if l == nil {
		return errors.New("s3store: nil lease")
	}

	objectKey := s.leaseKey(l.Resource)
	current, etag, err := s.getLease(ctx, objectKey)
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
	if err := s.putLease(ctx, objectKey, released, etag); err != nil {
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
func (s *Store) Append(ctx context.Context, key string, epoch lease.Epoch, e journal.Entry) error {
	if key == "" {
		return fmt.Errorf("%w: empty key", invocation.ErrInvalid)
	}
	if e.Step < 0 {
		return fmt.Errorf("s3store: negative step %d", e.Step)
	}

	body, err := json.Marshal(entryRecord{Entry: e, Epoch: epoch})
	if err != nil {
		return fmt.Errorf("s3store: marshal entry: %w", err)
	}

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(s.entryKey(key, e.Step)),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
		// The fence. This condition is atomic, so one writer wins the
		// step whatever the lease says, and the entry never changes.
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return s.classifyConflict(ctx, key, e)
		}
		return fmt.Errorf("s3store: put entry: %w", err)
	}
	return nil
}

// classifyConflict tells a repeat of a step from a different step that
// took its place. Only the first is safe for the caller to adopt.
func (s *Store) classifyConflict(ctx context.Context, key string, e journal.Entry) error {
	stored, err := s.getEntry(ctx, s.entryKey(key, e.Step))
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
func (s *Store) Read(ctx context.Context, key string) iter.Seq2[journal.Entry, error] {
	return func(yield func(journal.Entry, error) bool) {
		if key == "" {
			yield(journal.Entry{}, fmt.Errorf("%w: empty key", invocation.ErrInvalid))
			return
		}

		pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(s.entriesPrefix(key)),
		})

		for pages.HasMorePages() {
			page, err := pages.NextPage(ctx)
			if err != nil {
				yield(journal.Entry{}, fmt.Errorf("s3store: list entries: %w", err))
				return
			}
			for _, obj := range page.Contents {
				e, err := s.getEntry(ctx, aws.ToString(obj.Key))
				if !yield(e, err) || err != nil {
					return
				}
			}
		}
	}
}

// getObject reads one object whole. It returns the raw error, so a
// caller can tell a missing object from a failure.
func (s *Store) getObject(ctx context.Context, objectKey string) ([]byte, string, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = out.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(out.Body, maxObjectSize+1))
	if err != nil {
		return nil, "", fmt.Errorf("s3store: read %q: %w", objectKey, err)
	}
	if len(body) > maxObjectSize {
		return nil, "", fmt.Errorf("s3store: object %q is larger than %d bytes", objectKey, maxObjectSize)
	}
	return body, aws.ToString(out.ETag), nil
}

func (s *Store) getEntry(ctx context.Context, objectKey string) (journal.Entry, error) {
	body, _, err := s.getObject(ctx, objectKey)
	if err != nil {
		return journal.Entry{}, fmt.Errorf("s3store: get entry %q: %w", objectKey, err)
	}

	var rec entryRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		return journal.Entry{}, fmt.Errorf("s3store: unmarshal entry %q: %w", objectKey, err)
	}
	return rec.Entry, nil
}

// getLease returns the stored lease and its ETag. It returns a nil
// record if no lease object exists yet.
func (s *Store) getLease(ctx context.Context, objectKey string) (*leaseRecord, string, error) {
	body, etag, err := s.getObject(ctx, objectKey)
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
func (s *Store) putLease(ctx context.Context, objectKey string, rec leaseRecord, etag string) error {
	body, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("s3store: marshal lease: %w", err)
	}

	in := &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(objectKey),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
	}
	if etag == "" {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = aws.String(etag)
	}

	if _, err := s.client.PutObject(ctx, in); err != nil {
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
