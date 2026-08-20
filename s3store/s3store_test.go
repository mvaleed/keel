package s3store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// newJournal returns a journal that has its own fresh bucket.
func newStore(t *testing.T) *s3store.Store {
	t.Helper()
	c, err := client()
	if err != nil {
		t.Fatalf("minio: %v", err)
	}

	j, err := s3store.New(c, newBucket(t, c), "keel")
	if err != nil {
		t.Fatalf("new journal: %v", err)
	}
	return j
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
	j := newStore(t)
	ctx := t.Context()

	held, err := j.Claim(ctx, "svc/h/inv-1", "worker-a", time.Minute)
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
		if err := j.Append(ctx, held.Resource, held.Epoch, e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	got, err := journal.Collect(j.Read(ctx, "svc/h/inv-1"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertEntries(t, got, want)
}

func TestReadEmptyInvocation(t *testing.T) {
	t.Parallel()
	j := newStore(t)

	got, err := journal.Collect(j.Read(t.Context(), "svc/h/missing"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d entries, want 0", len(got))
	}
}

func TestClaimHeldByOtherOwner(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	if _, err := j.Claim(ctx, "svc/h/inv-2", "worker-a", time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := j.Claim(ctx, "svc/h/inv-2", "worker-b", time.Minute); !errors.Is(err, lease.ErrClaimHeld) {
		t.Fatalf("second claim err = %v, want ErrClaimHeld", err)
	}
}

func TestClaimBySameOwnerBumpsEpoch(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	first, err := j.Claim(ctx, "svc/h/inv-3", "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	second, err := j.Claim(ctx, "svc/h/inv-3", "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if second.Epoch != first.Epoch+1 {
		t.Fatalf("epoch = %d, want %d", second.Epoch, first.Epoch+1)
	}
}

func TestClaimAfterExpiry(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	if _, err := j.Claim(ctx, "svc/h/inv-4", "worker-a", 50*time.Millisecond); err != nil {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	held, err := j.Claim(ctx, "svc/h/inv-4", "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("claim after expiry: %v", err)
	}
	if held.Epoch != 2 {
		t.Fatalf("epoch = %d, want 2", held.Epoch)
	}
}

func TestReleaseLetsAnotherOwnerClaim(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	held, err := j.Claim(ctx, "svc/h/inv-5", "worker-a", time.Hour)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := j.Release(ctx, held); err != nil {
		t.Fatalf("release: %v", err)
	}
	// A second release of the same held must stay silent.
	if err := j.Release(ctx, held); err != nil {
		t.Fatalf("second release: %v", err)
	}

	next, err := j.Claim(ctx, "svc/h/inv-5", "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("claim after release: %v", err)
	}
	if next.Epoch != held.Epoch+1 {
		t.Fatalf("epoch = %d, want %d", next.Epoch, held.Epoch+1)
	}
}

func TestAppendDoesNotReadTheLease(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	held, err := j.Claim(ctx, "svc/h/inv-6", "worker-a", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// The lease is expired, and the write still lands. The fence is the
	// conditional write on the step, not the lease.
	want := journal.Entry{Step: 0, Name: "late"}
	if err := j.Append(ctx, held.Resource, held.Epoch, want); err != nil {
		t.Fatalf("append with an expired lease: %v", err)
	}

	got, err := journal.Collect(j.Read(ctx, "svc/h/inv-6"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertEntries(t, got, []journal.Entry{want})
}

func TestReadOrdersEntriesByEpoch(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	first, err := j.Claim(ctx, "svc/h/inv-7", "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Append out of step order, so the test proves Read sorts by step.
	for _, step := range []int{1, 0} {
		if err := j.Append(ctx, first.Resource, first.Epoch, journal.Entry{Step: step, Name: "a"}); err != nil {
			t.Fatalf("append %d: %v", step, err)
		}
	}

	if err := j.Release(ctx, first); err != nil {
		t.Fatalf("release: %v", err)
	}
	// A takeover continues the same history at a later step.
	second, err := j.Claim(ctx, "svc/h/inv-7", "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if err := j.Append(ctx, second.Resource, second.Epoch, journal.Entry{Step: 2, Name: "b"}); err != nil {
		t.Fatalf("append 2: %v", err)
	}

	got, err := journal.Collect(j.Read(ctx, "svc/h/inv-7"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertEntries(t, got, []journal.Entry{
		{Step: 0, Name: "a"}, {Step: 1, Name: "a"}, {Step: 2, Name: "b"},
	})
}

func TestAppendSameStepTwiceIsRejected(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	held, err := j.Claim(ctx, "svc/h/inv-13", "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	want := journal.Entry{Step: 0, Name: "charge", Output: json.RawMessage(`{"id":"first"}`)}
	if err := j.Append(ctx, held.Resource, held.Epoch, want); err != nil {
		t.Fatalf("first append: %v", err)
	}

	second := journal.Entry{Step: 0, Name: "charge", Output: json.RawMessage(`{"id":"second"}`)}
	if err := j.Append(ctx, held.Resource, held.Epoch, second); !errors.Is(err, journal.ErrStepExists) {
		t.Fatalf("second append err = %v, want ErrStepExists", err)
	}

	// The first writer's value must survive, so a replay stays stable.
	got, err := journal.Collect(j.Read(ctx, "svc/h/inv-13"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertEntries(t, got, []journal.Entry{want})
}

func TestStaleHolderCannotForkHistory(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	stale, err := j.Claim(ctx, "svc/h/inv-14", "worker-a", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := j.Append(ctx, stale.Resource, stale.Epoch, journal.Entry{Step: 0, Name: "done"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	fresh, err := j.Claim(ctx, "svc/h/inv-14", "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if err := j.Append(ctx, fresh.Resource, fresh.Epoch, journal.Entry{Step: 1, Name: "next"}); err != nil {
		t.Fatalf("append after takeover: %v", err)
	}

	// The step is taken, so the conditional write rejects the stale
	// holder and the recorded entry does not change.
	if err := j.Append(ctx, stale.Resource, stale.Epoch, journal.Entry{Step: 1, Name: "zombie"}); !errors.Is(err, journal.ErrNonDeterministic) {
		t.Fatalf("zombie append err = %v, want ErrNonDeterministic", err)
	}
	// A repeat of the step the new holder wrote is safe to adopt.
	if err := j.Append(ctx, stale.Resource, stale.Epoch, journal.Entry{Step: 1, Name: "next"}); !errors.Is(err, journal.ErrStepExists) {
		t.Fatalf("zombie repeat err = %v, want ErrStepExists", err)
	}

	got, err := journal.Collect(j.Read(ctx, "svc/h/inv-14"))
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
	j := newStore(t)
	ctx := t.Context()

	stale, err := j.Claim(ctx, "svc/h/inv-19", "worker-a", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	fresh, err := j.Claim(ctx, "svc/h/inv-19", "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}

	if err := j.Append(ctx, stale.Resource, stale.Epoch, journal.Entry{Step: 0, Name: "raced"}); err != nil {
		t.Fatalf("stale append: %v", err)
	}
	// The new holder now finds the step taken, and adopts it.
	if err := j.Append(ctx, fresh.Resource, fresh.Epoch, journal.Entry{Step: 0, Name: "raced"}); !errors.Is(err, journal.ErrStepExists) {
		t.Fatalf("append err = %v, want ErrStepExists", err)
	}

	got, err := journal.Collect(j.Read(ctx, "svc/h/inv-19"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertEntries(t, got, []journal.Entry{{Step: 0, Name: "raced"}})
}

func TestRenewKeepsEpochAndExtendsLease(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	held, err := j.Claim(ctx, "svc/h/inv-15", "worker-a", 300*time.Millisecond)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	epoch, expires := held.Epoch, held.Expires()

	time.Sleep(150 * time.Millisecond)
	if err := j.Renew(ctx, held, time.Minute); err != nil {
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
	if err := j.Append(ctx, held.Resource, held.Epoch, journal.Entry{Step: 0, Name: "late"}); err != nil {
		t.Fatalf("append after renew: %v", err)
	}
	// The renewal must not have let another owner in.
	if _, err := j.Claim(ctx, "svc/h/inv-15", "worker-b", time.Minute); !errors.Is(err, lease.ErrClaimHeld) {
		t.Fatalf("claim err = %v, want ErrClaimHeld", err)
	}
}

func TestRenewAfterTakeoverFails(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	held, err := j.Claim(ctx, "svc/h/inv-16", "worker-a", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := j.Claim(ctx, "svc/h/inv-16", "worker-b", time.Minute); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	if err := j.Renew(ctx, held, time.Minute); !errors.Is(err, lease.ErrLeaseLost) {
		t.Fatalf("renew err = %v, want ErrLeaseLost", err)
	}
}

func TestHeartbeatHoldsLeaseAcrossLongCall(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	const ttl = 300 * time.Millisecond
	held, err := j.Claim(ctx, "svc/h/inv-17", "worker-a", ttl)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	stop := lease.Keepalive(ctx, j, held, ttl)
	// Stand in for a service call that outlives the ttl.
	time.Sleep(3 * ttl)

	if err := j.Append(ctx, held.Resource, held.Epoch, journal.Entry{Step: 0, Name: "slow"}); err != nil {
		t.Fatalf("append under heartbeat: %v", err)
	}
	if err := stop(); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	// A second stop must repeat the same answer, not block.
	if err := stop(); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

func TestHeartbeatReportsLostLease(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	const ttl = 300 * time.Millisecond
	held, err := j.Claim(ctx, "svc/h/inv-18", "worker-a", ttl)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := j.Release(ctx, held); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := j.Claim(ctx, "svc/h/inv-18", "worker-b", time.Minute); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	stop := lease.Keepalive(ctx, j, held, ttl)
	// Wait past the first renewal, which must find the held gone.
	time.Sleep(2 * ttl)
	if err := stop(); !errors.Is(err, lease.ErrLeaseLost) {
		t.Fatalf("heartbeat err = %v, want ErrLeaseLost", err)
	}
}

func TestConcurrentClaimGivesOneWinner(t *testing.T) {
	t.Parallel()
	j := newStore(t)
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
			l, err := j.Claim(ctx, "svc/h/inv-8", fmt.Sprintf("worker-%d", i), time.Minute)
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
	j := newStore(t)
	ctx := t.Context()

	held, err := j.Claim(ctx, "svc/h/inv-19", "worker-a", time.Minute)
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
			err := j.Append(ctx, held.Resource, held.Epoch, journal.Entry{
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

	got, err := journal.Collect(j.Read(ctx, "svc/h/inv-19"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}
}

func TestConcurrentAppendKeepsEveryEntry(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	held, err := j.Claim(ctx, "svc/h/inv-9", "worker-a", time.Minute)
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
			errs[i] = j.Append(ctx, held.Resource, held.Epoch, journal.Entry{Step: i, Name: fmt.Sprintf("step-%d", i)})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	got, err := journal.Collect(j.Read(ctx, "svc/h/inv-9"))
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
	j := newStore(t)
	ctx := t.Context()

	held, err := j.Claim(ctx, "svc/h/inv-10", "worker-a", 10*time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	// MinIO returns at most 1000 keys in one page.
	const entries = 1100
	for i := range entries {
		if err := j.Append(ctx, held.Resource, held.Epoch, journal.Entry{Step: i}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	got, err := journal.Collect(j.Read(ctx, "svc/h/inv-10"))
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
	j := newStore(t)
	ctx := t.Context()

	for _, id := range []string{"svc/h/inv-11", "svc/h/inv-12"} {
		held, err := j.Claim(ctx, id, "worker-a", time.Minute)
		if err != nil {
			t.Fatalf("claim %s: %v", id, err)
		}
		if err := j.Append(ctx, held.Resource, held.Epoch, journal.Entry{Name: id}); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}

	for _, id := range []string{"svc/h/inv-11", "svc/h/inv-12"} {
		got, err := journal.Collect(j.Read(ctx, id))
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
		t.Fatalf("new journal: %v", err)
	}
	// The rival uses a plain client, so only the release is hooked.
	rival, err := s3store.New(base, bucket, "keel")
	if err != nil {
		t.Fatalf("new rival journal: %v", err)
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
	j := newStore(t)
	ctx := t.Context()

	held, err := j.Claim(ctx, "svc/h/inv-21", "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	recorded := journal.Entry{Step: 1, Name: "charge", Output: json.RawMessage(`{"id":"pay_1"}`)}
	if err := j.Append(ctx, held.Resource, held.Epoch, recorded); err != nil {
		t.Fatalf("append: %v", err)
	}

	// A handler that gained a step shifts "charge" to another position,
	// so step 1 now replays as a different step.
	shifted := journal.Entry{Step: 1, Name: "send_receipt"}
	err = j.Append(ctx, held.Resource, held.Epoch, shifted)
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
	got, err := journal.Collect(j.Read(ctx, "svc/h/inv-21"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertEntries(t, got, []journal.Entry{recorded})
}

func TestAppendSameNameAtSameStepIsAdoptable(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	held, err := j.Claim(ctx, "svc/h/inv-22", "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := j.Append(ctx, held.Resource, held.Epoch, journal.Entry{Step: 0, Name: "charge"}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// A retry of the same step is benign, so it stays ErrStepExists.
	err = j.Append(ctx, held.Resource, held.Epoch, journal.Entry{Step: 0, Name: "charge", Output: json.RawMessage(`2`)})
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
	j := newStore(t)
	ctx := t.Context()

	want := record("order-1", `{"amount":5}`)
	if err := j.Create(ctx, want); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := j.Get(ctx, want.Key())
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
	j := newStore(t)
	ctx := t.Context()

	first := record("order-2", `{"amount":5}`)
	if err := j.Create(ctx, first); err != nil {
		t.Fatalf("create: %v", err)
	}

	// A second registration of one address must not start a second run,
	// whatever it carries.
	second := record("order-2", `{"amount":9999}`)
	if err := j.Create(ctx, second); !errors.Is(err, invocation.ErrExists) {
		t.Fatalf("second create err = %v, want ErrExists", err)
	}

	got, err := j.Get(ctx, first.Key())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got.Input) != `{"amount":5}` {
		t.Fatalf("input = %s, want the first input", got.Input)
	}
}

func TestGetUnknownRecord(t *testing.T) {
	t.Parallel()
	j := newStore(t)

	if _, err := j.Get(t.Context(), "billing/Charge/never"); !errors.Is(err, invocation.ErrNotFound) {
		t.Fatalf("get err = %v, want ErrNotFound", err)
	}
}

func TestCreateRejectsAnInvalidKey(t *testing.T) {
	t.Parallel()
	j := newStore(t)

	bad := record("../../escape", `{}`)
	if err := j.Create(t.Context(), bad); !errors.Is(err, invocation.ErrInvalid) {
		t.Fatalf("create err = %v, want ErrInvalid", err)
	}
}

func TestPendingListsEveryNewRecord(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	want := map[string]bool{}
	for _, id := range []string{"order-10", "order-11", "order-12"} {
		r := record(id, `{}`)
		if err := j.Create(ctx, r); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		want[r.Key()] = true
	}

	got := map[string]bool{}
	for key, err := range j.Pending(ctx) {
		if err != nil {
			t.Fatalf("pending: %v", err)
		}
		got[key] = true
	}

	if len(got) != len(want) {
		t.Fatalf("pending = %v, want %v", got, want)
	}
	for key := range want {
		if !got[key] {
			t.Fatalf("pending %v is missing %q", got, key)
		}
	}
}

func TestPendingKeysAreReadable(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	want := record("order-13", `{"a":1}`)
	if err := j.Create(ctx, want); err != nil {
		t.Fatalf("create: %v", err)
	}

	// A dispatcher reads the record for every key a scan gives it, so a
	// yielded key must always address one.
	for key, err := range j.Pending(ctx) {
		if err != nil {
			t.Fatalf("pending: %v", err)
		}
		got, err := j.Get(ctx, key)
		if err != nil {
			t.Fatalf("get %q: %v", key, err)
		}
		if got.ID != want.ID {
			t.Fatalf("record = %+v, want %s", got.Invocation, want.ID)
		}
	}
}

func TestClearPending(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	r := record("order-14", `{}`)
	if err := j.Create(ctx, r); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := j.ClearPending(ctx, r.Key()); err != nil {
		t.Fatalf("clear: %v", err)
	}

	for key, err := range j.Pending(ctx) {
		if err != nil {
			t.Fatalf("pending: %v", err)
		}
		t.Fatalf("pending still holds %q", key)
	}
	// Clearing the index must not touch the record itself.
	if _, err := j.Get(ctx, r.Key()); err != nil {
		t.Fatalf("get after clear: %v", err)
	}
	// A repeat clear is silent, so a dispatcher can retry it.
	if err := j.ClearPending(ctx, r.Key()); err != nil {
		t.Fatalf("second clear: %v", err)
	}
}

func TestPendingIsEmptyForANewBucket(t *testing.T) {
	t.Parallel()
	j := newStore(t)

	for key, err := range j.Pending(t.Context()) {
		if err != nil {
			t.Fatalf("pending: %v", err)
		}
		t.Fatalf("pending holds %q, want nothing", key)
	}
}

func TestRecordAndJournalShareOneSubtree(t *testing.T) {
	t.Parallel()
	j := newStore(t)
	ctx := t.Context()

	r := record("order-15", `{}`)
	if err := j.Create(ctx, r); err != nil {
		t.Fatalf("create: %v", err)
	}
	held, err := j.Claim(ctx, r.Key(), "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := j.Append(ctx, r.Key(), held.Epoch, journal.Entry{Step: 0, Name: "charge"}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// One address reaches the record, the lease, and the journal.
	entries, err := journal.Collect(j.Read(ctx, r.Key()))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	assertEntries(t, entries, []journal.Entry{{Step: 0, Name: "charge"}})
}
