package s3store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/keel/keel/invocation"
	"github.com/keel/keel/journal"
	"github.com/keel/keel/lease"
	"github.com/keel/keel/s3store"
)

// minioImage must support conditional writes, so it uses If-None-Match
// and If-Match on PutObject.
const minioImage = "minio/minio:RELEASE.2025-04-22T22-12-26Z"

// client starts one MinIO container for the whole package and returns an
// S3 client that points at it.
var client = sync.OnceValues(startMinio)

func startMinio() (*s3.Client, error) {
	ctx := context.Background()
	container, err := tcminio.Run(ctx, minioImage)
	if err != nil {
		return nil, fmt.Errorf("start minio: %w", err)
	}
	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		return nil, fmt.Errorf("minio endpoint: %w", err)
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			container.Username, container.Password, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + endpoint)
		o.UsePathStyle = true
	}), nil
}

// newStore returns a store that has its own fresh bucket.
func newStore(t *testing.T) *s3store.Store {
	t.Helper()
	c, err := client()
	if err != nil {
		t.Fatalf("minio: %v", err)
	}

	store, err := s3store.New(c, newBucket(t, c), "keel")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return store
}

// newBucket creates a bucket that only the calling test uses.
func newBucket(t *testing.T, c *s3.Client) string {
	t.Helper()
	bucket := fmt.Sprintf("keel-%d-%d", time.Now().UnixNano(), bucketSeq.Add(1))
	if _, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return bucket
}

// bucketSeq keeps two buckets made in the same nanosecond apart.
var bucketSeq atomic.Uint64

func TestClaimAndAppendAndRead(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	held, err := store.Claim(ctx, "svc/h/inv-1", "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if held.Epoch != 1 {
		t.Fatalf("epoch = %d, want 1", held.Epoch)
	}

	want := []journal.Entry{
		{Step: 0, Name: "charge", Output: json.RawMessage(`{"ok":true}`)},
		{Step: 1, Name: "ship", Err: "carrier down"},
	}
	for _, e := range want {
		if err := store.Append(ctx, held.Resource, held.Epoch, e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	got, err := journal.Collect(store.Read(ctx, "svc/h/inv-1"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertEntries(t, got, want)
}

func TestReadEmptyInvocation(t *testing.T) {
	t.Parallel()
	store := newStore(t)

	got, err := journal.Collect(store.Read(t.Context(), "svc/h/missing"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d entries, want 0", len(got))
	}
}

func TestClaimHeldByOtherOwner(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	if _, err := store.Claim(ctx, "svc/h/inv-2", "worker-a", time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := store.Claim(ctx, "svc/h/inv-2", "worker-b", time.Minute); !errors.Is(err, lease.ErrClaimHeld) {
		t.Fatalf("second claim err = %v, want ErrClaimHeld", err)
	}
}

func TestClaimBySameOwnerBumpsEpoch(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	first, err := store.Claim(ctx, "svc/h/inv-3", "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	second, err := store.Claim(ctx, "svc/h/inv-3", "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if second.Epoch != first.Epoch+1 {
		t.Fatalf("epoch = %d, want %d", second.Epoch, first.Epoch+1)
	}
}

func TestClaimAfterExpiry(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	if _, err := store.Claim(ctx, "svc/h/inv-4", "worker-a", 50*time.Millisecond); err != nil {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	held, err := store.Claim(ctx, "svc/h/inv-4", "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("claim after expiry: %v", err)
	}
	if held.Epoch != 2 {
		t.Fatalf("epoch = %d, want 2", held.Epoch)
	}
}

func TestReleaseLetsAnotherOwnerClaim(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	held, err := store.Claim(ctx, "svc/h/inv-5", "worker-a", time.Hour)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.Release(ctx, held); err != nil {
		t.Fatalf("release: %v", err)
	}
	// A second release of the same held must stay silent.
	if err := store.Release(ctx, held); err != nil {
		t.Fatalf("second release: %v", err)
	}

	next, err := store.Claim(ctx, "svc/h/inv-5", "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("claim after release: %v", err)
	}
	if next.Epoch != held.Epoch+1 {
		t.Fatalf("epoch = %d, want %d", next.Epoch, held.Epoch+1)
	}
}

func TestAppendDoesNotReadTheLease(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	held, err := store.Claim(ctx, "svc/h/inv-6", "worker-a", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// The lease is expired, and the write still lands. The fence is the
	// conditional write on the step, not the lease.
	want := journal.Entry{Step: 0, Name: "late"}
	if err := store.Append(ctx, held.Resource, held.Epoch, want); err != nil {
		t.Fatalf("append with an expired lease: %v", err)
	}

	got, err := journal.Collect(store.Read(ctx, "svc/h/inv-6"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertEntries(t, got, []journal.Entry{want})
}

func TestReadOrdersEntriesByEpoch(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	first, err := store.Claim(ctx, "svc/h/inv-7", "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Append out of step order, so the test proves Read sorts by step.
	for _, step := range []int{1, 0} {
		if err := store.Append(ctx, first.Resource, first.Epoch, journal.Entry{Step: step, Name: "a"}); err != nil {
			t.Fatalf("append %d: %v", step, err)
		}
	}

	if err := store.Release(ctx, first); err != nil {
		t.Fatalf("release: %v", err)
	}
	// A takeover continues the same history at a later step.
	second, err := store.Claim(ctx, "svc/h/inv-7", "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if err := store.Append(ctx, second.Resource, second.Epoch, journal.Entry{Step: 2, Name: "b"}); err != nil {
		t.Fatalf("append 2: %v", err)
	}

	got, err := journal.Collect(store.Read(ctx, "svc/h/inv-7"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertEntries(t, got, []journal.Entry{
		{Step: 0, Name: "a"}, {Step: 1, Name: "a"}, {Step: 2, Name: "b"},
	})
}

func TestAppendSameStepTwiceIsRejected(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	held, err := store.Claim(ctx, "svc/h/inv-13", "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	want := journal.Entry{Step: 0, Name: "charge", Output: json.RawMessage(`{"id":"first"}`)}
	if err := store.Append(ctx, held.Resource, held.Epoch, want); err != nil {
		t.Fatalf("first append: %v", err)
	}

	second := journal.Entry{Step: 0, Name: "charge", Output: json.RawMessage(`{"id":"second"}`)}
	if err := store.Append(ctx, held.Resource, held.Epoch, second); !errors.Is(err, journal.ErrStepExists) {
		t.Fatalf("second append err = %v, want ErrStepExists", err)
	}

	// The first writer's value must survive, so a replay stays stable.
	got, err := journal.Collect(store.Read(ctx, "svc/h/inv-13"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertEntries(t, got, []journal.Entry{want})
}

func TestStaleHolderCannotForkHistory(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	stale, err := store.Claim(ctx, "svc/h/inv-14", "worker-a", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.Append(ctx, stale.Resource, stale.Epoch, journal.Entry{Step: 0, Name: "done"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	fresh, err := store.Claim(ctx, "svc/h/inv-14", "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if err := store.Append(ctx, fresh.Resource, fresh.Epoch, journal.Entry{Step: 1, Name: "next"}); err != nil {
		t.Fatalf("append after takeover: %v", err)
	}

	// The step is taken, so the conditional write rejects the stale
	// holder and the recorded entry does not change.
	if err := store.Append(ctx, stale.Resource, stale.Epoch, journal.Entry{Step: 1, Name: "zombie"}); !errors.Is(err, journal.ErrNonDeterministic) {
		t.Fatalf("zombie append err = %v, want ErrNonDeterministic", err)
	}
	// A repeat of the step the new holder wrote is safe to adopt.
	if err := store.Append(ctx, stale.Resource, stale.Epoch, journal.Entry{Step: 1, Name: "next"}); !errors.Is(err, journal.ErrStepExists) {
		t.Fatalf("zombie repeat err = %v, want ErrStepExists", err)
	}

	got, err := journal.Collect(store.Read(ctx, "svc/h/inv-14"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertEntries(t, got, []journal.Entry{
		{Step: 0, Name: "done"}, {Step: 1, Name: "next"},
	})
}

// A stale holder that reaches a step first wins it. Append does not
// stop this, because the epoch is not checked; the holder must stop
// itself when it loses the lease.
func TestStaleHolderWinsAnUnwrittenStep(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	stale, err := store.Claim(ctx, "svc/h/inv-19", "worker-a", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	fresh, err := store.Claim(ctx, "svc/h/inv-19", "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}

	if err := store.Append(ctx, stale.Resource, stale.Epoch, journal.Entry{Step: 0, Name: "raced"}); err != nil {
		t.Fatalf("stale append: %v", err)
	}
	// The new holder now finds the step taken, and adopts it.
	if err := store.Append(ctx, fresh.Resource, fresh.Epoch, journal.Entry{Step: 0, Name: "raced"}); !errors.Is(err, journal.ErrStepExists) {
		t.Fatalf("append err = %v, want ErrStepExists", err)
	}

	got, err := journal.Collect(store.Read(ctx, "svc/h/inv-19"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertEntries(t, got, []journal.Entry{{Step: 0, Name: "raced"}})
}

func TestRenewKeepsEpochAndExtendsLease(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	held, err := store.Claim(ctx, "svc/h/inv-15", "worker-a", 300*time.Millisecond)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	epoch, expires := held.Epoch, held.Expires()

	time.Sleep(150 * time.Millisecond)
	if err := store.Renew(ctx, held, time.Minute); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if held.Epoch != epoch {
		t.Fatalf("epoch = %d, want %d", held.Epoch, epoch)
	}
	if !held.Expires().After(expires) {
		t.Fatalf("expiry %v did not move past %v", held.Expires(), expires)
	}

	// Past the original ttl the renewed held must still append.
	time.Sleep(200 * time.Millisecond)
	if err := store.Append(ctx, held.Resource, held.Epoch, journal.Entry{Step: 0, Name: "late"}); err != nil {
		t.Fatalf("append after renew: %v", err)
	}
	// The renewal must not have let another owner in.
	if _, err := store.Claim(ctx, "svc/h/inv-15", "worker-b", time.Minute); !errors.Is(err, lease.ErrClaimHeld) {
		t.Fatalf("claim err = %v, want ErrClaimHeld", err)
	}
}

func TestRenewAfterTakeoverFails(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	held, err := store.Claim(ctx, "svc/h/inv-16", "worker-a", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := store.Claim(ctx, "svc/h/inv-16", "worker-b", time.Minute); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	if err := store.Renew(ctx, held, time.Minute); !errors.Is(err, lease.ErrLeaseLost) {
		t.Fatalf("renew err = %v, want ErrLeaseLost", err)
	}
}

func TestRepeatedRenewHoldsALongCall(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	// A driver renews while its attempt makes progress, so the backend
	// must hold one lease across a call that outlives the ttl.
	const ttl = 300 * time.Millisecond
	held, err := store.Claim(ctx, "svc/h/inv-17", "worker-a", ttl)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	for range 3 {
		time.Sleep(ttl / 3)
		if err := store.Renew(ctx, held, ttl); err != nil {
			t.Fatalf("renew: %v", err)
		}
	}

	if err := store.Append(ctx, held.Resource, held.Epoch, journal.Entry{Step: 0, Name: "slow"}); err != nil {
		t.Fatalf("append under a renewed lease: %v", err)
	}
	// No renewal may let another owner in.
	if _, err := store.Claim(ctx, "svc/h/inv-17", "worker-b", time.Minute); !errors.Is(err, lease.ErrClaimHeld) {
		t.Fatalf("claim err = %v, want ErrClaimHeld", err)
	}
}

func TestConcurrentClaimGivesOneWinner(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	const claimants = 8
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		leases  []*lease.Lease
		held    int
		unknown []error
	)
	for i := range claimants {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := store.Claim(ctx, "svc/h/inv-8", fmt.Sprintf("worker-%d", i), time.Minute)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				leases = append(leases, l)
			case errors.Is(err, lease.ErrClaimHeld):
				held++
			default:
				unknown = append(unknown, err)
			}
		}()
	}
	wg.Wait()

	if len(unknown) > 0 {
		t.Fatalf("unexpected claim errors: %v", unknown)
	}
	if len(leases) != 1 {
		t.Fatalf("got %d winners, want 1", len(leases))
	}
	if held != claimants-1 {
		t.Fatalf("got %d held errors, want %d", held, claimants-1)
	}
}

func TestConcurrentAppendToOneStepHasOneWinner(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	held, err := store.Claim(ctx, "svc/h/inv-19", "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	const writers = 8
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		won     int
		existed int
		unknown []error
	)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// One step, one name: this tests the fence, not the name.
			err := store.Append(ctx, held.Resource, held.Epoch, journal.Entry{
				Step:   0,
				Name:   "charge",
				Output: json.RawMessage(fmt.Sprintf("%d", i)),
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won++
			case errors.Is(err, journal.ErrStepExists):
				existed++
			default:
				unknown = append(unknown, err)
			}
		}()
	}
	wg.Wait()

	if len(unknown) > 0 {
		t.Fatalf("unexpected append errors: %v", unknown)
	}
	if won != 1 {
		t.Fatalf("got %d winners, want 1", won)
	}
	if existed != writers-1 {
		t.Fatalf("got %d ErrStepExists, want %d", existed, writers-1)
	}

	got, err := journal.Collect(store.Read(ctx, "svc/h/inv-19"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}
}

func TestConcurrentAppendKeepsEveryEntry(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	held, err := store.Claim(ctx, "svc/h/inv-9", "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	const appends = 16
	var wg sync.WaitGroup
	errs := make([]error, appends)
	for i := range appends {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = store.Append(ctx, held.Resource, held.Epoch, journal.Entry{Step: i, Name: fmt.Sprintf("step-%d", i)})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	got, err := journal.Collect(store.Read(ctx, "svc/h/inv-9"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != appends {
		t.Fatalf("got %d entries, want %d", len(got), appends)
	}
	seen := make(map[int]bool, appends)
	for _, e := range got {
		seen[e.Step] = true
	}
	if len(seen) != appends {
		t.Fatalf("got %d distinct steps, want %d", len(seen), appends)
	}
}

func TestReadPaginatesBeyondOnePage(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	held, err := store.Claim(ctx, "svc/h/inv-10", "worker-a", 10*time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	// MinIO returns at most 1000 keys in one page.
	const entries = 1100
	for i := range entries {
		if err := store.Append(ctx, held.Resource, held.Epoch, journal.Entry{Step: i}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	got, err := journal.Collect(store.Read(ctx, "svc/h/inv-10"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != entries {
		t.Fatalf("got %d entries, want %d", len(got), entries)
	}
	for i, e := range got {
		if e.Step != i {
			t.Fatalf("entry %d has step %d, want %d", i, e.Step, i)
		}
	}
}

func TestSeparateInvocationsDoNotMix(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	for _, id := range []string{"svc/h/inv-11", "svc/h/inv-12"} {
		held, err := store.Claim(ctx, id, "worker-a", time.Minute)
		if err != nil {
			t.Fatalf("claim %s: %v", id, err)
		}
		if err := store.Append(ctx, held.Resource, held.Epoch, journal.Entry{Name: id}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}

	for _, id := range []string{"svc/h/inv-11", "svc/h/inv-12"} {
		got, err := journal.Collect(store.Read(ctx, id))
		if err != nil {
			t.Fatalf("read %s: %v", id, err)
		}
		assertEntries(t, got, []journal.Entry{{Name: id}})
	}
}

func assertEntries(t *testing.T, got, want []journal.Entry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Step != want[i].Step || got[i].Name != want[i].Name || got[i].Err != want[i].Err {
			t.Fatalf("entry %d = %+v, want %+v", i, got[i], want[i])
		}
		if string(got[i].Output) != string(want[i].Output) {
			t.Fatalf("entry %d output = %s, want %s", i, got[i].Output, want[i].Output)
		}
	}
}

// raceClient is the real S3 client with a hook. It runs fn after the
// GetObject that Release makes, so the write lands in a lost race.
type raceClient struct {
	*s3.Client
	once  sync.Once
	armed atomic.Bool
	fn    func()
}

func (c *raceClient) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	out, err := c.Client.GetObject(ctx, in, opts...)
	if c.armed.Load() {
		c.once.Do(c.fn)
	}
	return out, err
}

func TestReleaseLosesRaceToNewClaim(t *testing.T) {
	t.Parallel()
	base, err := client()
	if err != nil {
		t.Fatalf("minio: %v", err)
	}
	bucket := newBucket(t, base)
	ctx := t.Context()

	racer := &raceClient{Client: base}
	slow, err := s3store.New(racer, bucket, "keel")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	// The rival uses a plain client, so only the release is hooked.
	rival, err := s3store.New(base, bucket, "keel")
	if err != nil {
		t.Fatalf("new rival store: %v", err)
	}

	held, err := slow.Claim(ctx, "svc/h/inv-20", "worker-a", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Release ignores the expiry, so the guard still passes and the
	// conditional write is what has to reject the stale holder.
	time.Sleep(100 * time.Millisecond)

	racer.fn = func() {
		if _, err := rival.Claim(ctx, "svc/h/inv-20", "worker-b", time.Minute); err != nil {
			t.Errorf("rival claim: %v", err)
		}
	}
	racer.armed.Store(true)

	if err := slow.Release(ctx, held); err != nil {
		t.Fatalf("release after lost race: %v", err)
	}
	racer.armed.Store(false)

	// The rival's live held must survive the late release.
	if _, err := rival.Claim(ctx, "svc/h/inv-20", "worker-c", time.Minute); !errors.Is(err, lease.ErrClaimHeld) {
		t.Fatalf("claim err = %v, want ErrClaimHeld", err)
	}
}

func TestAppendDifferentNameAtSameStepIsNonDeterministic(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	held, err := store.Claim(ctx, "svc/h/inv-21", "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	recorded := journal.Entry{Step: 1, Name: "charge", Output: json.RawMessage(`{"id":"pay_1"}`)}
	if err := store.Append(ctx, held.Resource, held.Epoch, recorded); err != nil {
		t.Fatalf("append: %v", err)
	}

	// A handler that gained a step shifts "charge" to another position,
	// so step 1 now replays as a different step.
	shifted := journal.Entry{Step: 1, Name: "send_receipt"}
	err = store.Append(ctx, held.Resource, held.Epoch, shifted)
	if !errors.Is(err, journal.ErrNonDeterministic) {
		t.Fatalf("append err = %v, want ErrNonDeterministic", err)
	}
	if errors.Is(err, journal.ErrStepExists) {
		t.Fatal("a shifted step must not look adoptable")
	}
	for _, want := range []string{"charge", "send_receipt", "step 1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}

	// The recorded step must survive the rejected write.
	got, err := journal.Collect(store.Read(ctx, "svc/h/inv-21"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertEntries(t, got, []journal.Entry{recorded})
}

func TestAppendSameNameAtSameStepIsAdoptable(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	held, err := store.Claim(ctx, "svc/h/inv-22", "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.Append(ctx, held.Resource, held.Epoch, journal.Entry{Step: 0, Name: "charge"}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// A retry of the same step is benign, so it stays ErrStepExists.
	err = store.Append(ctx, held.Resource, held.Epoch, journal.Entry{Step: 0, Name: "charge", Output: json.RawMessage(`2`)})
	if !errors.Is(err, journal.ErrStepExists) {
		t.Fatalf("append err = %v, want ErrStepExists", err)
	}
	if errors.Is(err, journal.ErrNonDeterministic) {
		t.Fatal("a repeat of one step must not look non-deterministic")
	}
}

func record(id string, input string) invocation.Record {
	return invocation.Record{
		Invocation: invocation.Invocation{
			ID: invocation.ID(id), Service: "billing", Handler: "Charge",
			Input: json.RawMessage(input),
		},
		Status:    invocation.Pending,
		InputHash: invocation.HashInput([]byte(input)),
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
}

func TestCreateAndGet(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	want := record("order-1", `{"amount":5}`)
	if err := store.Create(ctx, want); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := store.Get(ctx, want.Key())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != want.ID || got.Service != want.Service || got.Handler != want.Handler {
		t.Fatalf("record = %+v, want %+v", got.Invocation, want.Invocation)
	}
	if string(got.Input) != string(want.Input) {
		t.Fatalf("input = %s, want %s", got.Input, want.Input)
	}
	if got.Status != invocation.Pending {
		t.Fatalf("status = %q, want pending", got.Status)
	}
	if got.InputHash != want.InputHash {
		t.Fatalf("input hash = %q, want %q", got.InputHash, want.InputHash)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("created at = %v, want %v", got.CreatedAt, want.CreatedAt)
	}
}

func TestCreateIsWriteOnce(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	first := record("order-2", `{"amount":5}`)
	if err := store.Create(ctx, first); err != nil {
		t.Fatalf("create: %v", err)
	}

	// A second registration of one address must not start a second run,
	// whatever it carries.
	second := record("order-2", `{"amount":9999}`)
	if err := store.Create(ctx, second); !errors.Is(err, invocation.ErrExists) {
		t.Fatalf("second create err = %v, want ErrExists", err)
	}

	got, err := store.Get(ctx, first.Key())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got.Input) != `{"amount":5}` {
		t.Fatalf("input = %s, want the first input", got.Input)
	}
}

func TestGetUnknownRecord(t *testing.T) {
	t.Parallel()
	store := newStore(t)

	if _, err := store.Get(t.Context(), "billing/Charge/never"); !errors.Is(err, invocation.ErrNotFound) {
		t.Fatalf("get err = %v, want ErrNotFound", err)
	}
}

func TestCreateRejectsAnInvalidKey(t *testing.T) {
	t.Parallel()
	store := newStore(t)

	bad := record("../../escape", `{}`)
	if err := store.Create(t.Context(), bad); !errors.Is(err, invocation.ErrInvalid) {
		t.Fatalf("create err = %v, want ErrInvalid", err)
	}
}

// due collects the markers a scan yields at now.
func due(t *testing.T, store *s3store.Store, now time.Time) []invocation.WakeupMarker {
	t.Helper()
	var got []invocation.WakeupMarker
	for m, err := range store.Due(t.Context(), now) {
		if err != nil {
			t.Fatalf("due: %v", err)
		}
		got = append(got, m)
	}
	return got
}

func TestCreateSchedulesEveryNewRecord(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	want := map[string]bool{}
	for _, id := range []string{"order-10", "order-11", "order-12"} {
		r := record(id, `{}`)
		if err := store.Create(ctx, r); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		want[r.Key()] = true
	}

	got := map[string]bool{}
	for _, m := range due(t, store, time.Now().Add(time.Minute)) {
		got[m.Key] = true
	}

	if len(got) != len(want) {
		t.Fatalf("due = %v, want %v", got, want)
	}
	for key := range want {
		if !got[key] {
			t.Fatalf("due %v is missing %q", got, key)
		}
	}
}

func TestDueKeysAreReadable(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	want := record("order-13", `{"a":1}`)
	if err := store.Create(ctx, want); err != nil {
		t.Fatalf("create: %v", err)
	}

	// A dispatcher reads the record for every marker a scan gives it, so
	// a yielded marker must always address one.
	for _, m := range due(t, store, time.Now().Add(time.Minute)) {
		got, err := store.Get(ctx, m.Key)
		if err != nil {
			t.Fatalf("get %q: %v", m.Key, err)
		}
		if got.ID != want.ID {
			t.Fatalf("record = %+v, want %s", got.Invocation, want.ID)
		}
	}
}

func TestUnschedule(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	r := record("order-14", `{}`)
	if err := store.Create(ctx, r); err != nil {
		t.Fatalf("create: %v", err)
	}
	m := invocation.WakeupMarker{Key: r.Key(), Due: r.CreatedAt}
	if err := store.Forget(ctx, m); err != nil {
		t.Fatalf("unschedule: %v", err)
	}

	if got := due(t, store, time.Now().Add(time.Minute)); len(got) != 0 {
		t.Fatalf("due still holds %v", got)
	}
	// Dropping a marker must not touch the record itself.
	if _, err := store.Get(ctx, r.Key()); err != nil {
		t.Fatalf("get after unschedule: %v", err)
	}
	// A repeat is silent, so a dispatcher can retry it.
	if err := store.Forget(ctx, m); err != nil {
		t.Fatalf("second unschedule: %v", err)
	}
}

func TestDueIsEmptyForANewBucket(t *testing.T) {
	t.Parallel()
	store := newStore(t)

	if got := due(t, store, time.Now()); len(got) != 0 {
		t.Fatalf("due holds %v, want nothing", got)
	}
}

func TestDueReturnsTheEarliestMarkerFirst(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	for i, id := range []string{"late", "early", "middle"} {
		offset := map[string]time.Duration{"early": 0, "middle": time.Minute, "late": 2 * time.Minute}[id]
		_ = i
		if err := store.Schedule(ctx, "billing/Charge/"+id, base.Add(offset)); err != nil {
			t.Fatalf("schedule %s: %v", id, err)
		}
	}

	var got []string
	for _, m := range due(t, store, time.Now()) {
		got = append(got, m.Key)
	}
	want := []string{"billing/Charge/early", "billing/Charge/middle", "billing/Charge/late"}
	if !slices.Equal(got, want) {
		t.Fatalf("due = %v, want %v", got, want)
	}
}

func TestDueStopsAtNow(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	now := time.Now().Truncate(time.Second)
	if err := store.Schedule(ctx, "billing/Charge/ready", now.Add(-time.Second)); err != nil {
		t.Fatalf("schedule ready: %v", err)
	}
	if err := store.Schedule(ctx, "billing/Charge/later", now.Add(time.Hour)); err != nil {
		t.Fatalf("schedule later: %v", err)
	}

	got := due(t, store, now)
	if len(got) != 1 || got[0].Key != "billing/Charge/ready" {
		t.Fatalf("due = %v, want only the ready marker", got)
	}
}

func TestDueRoundTripsTheMarker(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	// A due time is named by whole seconds, so the marker returns the
	// truncated time and Forget addresses the same object.
	want := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := store.Schedule(ctx, "billing/Charge/rt", want); err != nil {
		t.Fatalf("schedule: %v", err)
	}

	got := due(t, store, time.Now())
	if len(got) != 1 {
		t.Fatalf("due = %v, want one marker", got)
	}
	if !got[0].Due.Equal(want) {
		t.Fatalf("due = %v, want %v", got[0].Due, want)
	}
	if err := store.Forget(ctx, got[0]); err != nil {
		t.Fatalf("unschedule: %v", err)
	}
	if rest := due(t, store, time.Now()); len(rest) != 0 {
		t.Fatalf("due still holds %v", rest)
	}
}

func TestUnscheduleLeavesADuplicateMarker(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	// A crash between Schedule and Forget leaves two markers for one
	// invocation. Dropping one must not drop the other.
	key := "billing/Charge/dup"
	first := time.Now().Add(-time.Hour).Truncate(time.Second)
	second := first.Add(time.Minute)
	for _, at := range []time.Time{first, second} {
		if err := store.Schedule(ctx, key, at); err != nil {
			t.Fatalf("schedule: %v", err)
		}
	}

	if err := store.Forget(ctx, invocation.WakeupMarker{Key: key, Due: first}); err != nil {
		t.Fatalf("unschedule: %v", err)
	}
	got := due(t, store, time.Now())
	if len(got) != 1 || !got[0].Due.Equal(second) {
		t.Fatalf("due = %v, want only the second marker", got)
	}
}

func TestUpdateReplacesTheRecord(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	r := record("order-15", `{}`)
	if err := store.Create(ctx, r); err != nil {
		t.Fatalf("create: %v", err)
	}

	r.Status = invocation.Succeeded
	r.Epoch = 3
	r.Output = json.RawMessage(`{"ok":true}`)
	if err := store.Update(ctx, r); err != nil {
		t.Fatalf("update: %v", err)
	}

	got, err := store.Get(ctx, r.Key())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != invocation.Succeeded || got.Epoch != 3 {
		t.Fatalf("record = %+v, want succeeded at epoch 3", got)
	}
}

func TestUpdateRejectsAStaleEpoch(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	r := record("order-16", `{}`)
	if err := store.Create(ctx, r); err != nil {
		t.Fatalf("create: %v", err)
	}

	live := r
	live.Epoch = 5
	live.Status = invocation.Running
	if err := store.Update(ctx, live); err != nil {
		t.Fatalf("update: %v", err)
	}

	stale := r
	stale.Epoch = 4
	stale.Status = invocation.Failed
	if err := store.Update(ctx, stale); !errors.Is(err, lease.ErrLeaseLost) {
		t.Fatalf("update err = %v, want ErrLeaseLost", err)
	}
}

func TestUpdateNeedsARecord(t *testing.T) {
	t.Parallel()
	store := newStore(t)

	if err := store.Update(t.Context(), record("order-17", `{}`)); !errors.Is(err, invocation.ErrNotFound) {
		t.Fatalf("update err = %v, want ErrNotFound", err)
	}
}

func TestRecordAndJournalShareOneSubtree(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	r := record("order-15", `{}`)
	if err := store.Create(ctx, r); err != nil {
		t.Fatalf("create: %v", err)
	}
	held, err := store.Claim(ctx, r.Key(), "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.Append(ctx, r.Key(), held.Epoch, journal.Entry{Step: 0, Name: "charge"}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// One address reaches the record, the lease, and the journal.
	entries, err := journal.Collect(store.Read(ctx, r.Key()))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertEntries(t, entries, []journal.Entry{{Step: 0, Name: "charge"}})
}
