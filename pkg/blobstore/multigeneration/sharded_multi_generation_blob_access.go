package multigeneration

import (
	"context"
	"log"
	"sync"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	emptyblobs "github.com/buildbarn/bb-storage/pkg/empty_blobs"
	"github.com/buildbarn/bb-storage/pkg/justbuild"
)

type ShardedMultiGenerationBlobAccess struct {
	nShards   uint32
	backends  []blobstore.BlobAccess
	semaphore chan struct{}
}

func NewShardedMultiGenerationBlobAccess(backends []blobstore.BlobAccess, concurrency uint32) *ShardedMultiGenerationBlobAccess {
	x := &ShardedMultiGenerationBlobAccess{
		nShards:   uint32(len(backends)),
		backends:  backends,
		semaphore: make(chan struct{}, concurrency),
	}
	return x
}

func (m *ShardedMultiGenerationBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	hash := digest.GetHashString()
	i := FNV(hash, m.nShards)
	b := m.backends[i].Get(ctx, digest)
	if justbuild.IsJustbuildTree(hash) {
		go m.traverse(hash, digest)
	}
	return b
}

func EntriesSet(bytes []byte, dgst digest.Digest) (digest.Set, error) {
	hashes, _, _, _ := justbuild.GetAllHashes(bytes)
	setBuilder := digest.NewSetBuilder()
	instName := dgst.GetInstanceName().String()
	for _, h := range hashes {
		curDgst := digest.MustNewDigest(instName, h, 0 /*sizeBytes*/)
		setBuilder.Add(curDgst)
	}
	return setBuilder.Build(), nil
}

func (m *ShardedMultiGenerationBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	hash := digest.GetHashString()
	i := FNV(hash, m.nShards)

	// simple blob
	if !justbuild.IsJustbuildTree(hash) {
		return m.backends[i].Put(ctx, digest, b)
	}

	// check if all leaves have been uploaded first
	//
	// need to duplicate the buffer because:
	// - one will be consumed to find the leaves
	// - the second one is consumed to store it into the CAS
	b1, b2 := b.CloneCopy(0)
	bytes, err := b1.ToByteSlice(int(digest.GetSizeBytes())) // first consumer
	if err != nil {
		return err
	}

	leaves, err := EntriesSet(bytes, digest)
	if err != nil {
		return err
	}

	missing, err := m.FindMissing(ctx, leaves)
	if err != nil {
		return err
	}
	if !missing.Empty() {
		log.Printf("incorrect tree upload detected. Tree %s has these missing direct dependencies %v\n", digest.GetHashString(), missing.Items())
	}
	err = m.backends[i].Put(ctx, digest, b2) // second consumer
	if err != nil {
		return err
	}

	// since a rotation could have happened between the upload of the
	// children and the root, we trigger one upstream of the whole tree.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		m.FindMissing(ctx, digest.ToSingletonSet())
	}()

	// all good
	return nil
}

func (m *ShardedMultiGenerationBlobAccess) traverse(treeHash string, dgst digest.Digest) {
	m.semaphore <- struct{}{}
	defer func() {
		<-m.semaphore
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	b := m.backends[FNV(treeHash, m.nShards)].Get(ctx, dgst)
	size, err := b.GetSizeBytes()
	if err != nil {
		log.Println(err)
		return
	}
	bytes, err := b.ToByteSlice(int(size))
	if err != nil {
		log.Println(err)
		return
	}
	entries, err := EntriesSet(bytes, dgst)
	if err != nil {
		log.Println(err)
		return
	}
	missing, err := m.FindMissing(ctx, entries)
	if err != nil {
		log.Println(err)
		return
	}
	if missing.Length() > 0 {
		log.Printf("incomplete tree detected %s: missing blobs are %v\n", dgst.GetHashString(), missing)
	}
}

func (m *ShardedMultiGenerationBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	digestsPerBackend := make([]digest.SetBuilder, 0, len(m.backends))
	for range m.backends {

		digestsPerBackend = append(digestsPerBackend, digest.NewSetBuilder())
	}
	for _, blobDigest := range digests.Items() {
		i := FNV(blobDigest.GetHashString(), m.nShards)
		digestsPerBackend[i].Add(blobDigest)
	}

	missingPerBackend := make([]digest.Set, 0, len(m.backends))
	for range m.backends {
		missingPerBackend = append(missingPerBackend, digest.EmptySet)
	}
	var wg sync.WaitGroup
	for idx, builderPerBackend := range digestsPerBackend {
		wg.Add(1)
		go func(idx int, digests digest.Set) {
			defer wg.Done()
			missing, _ := m.backends[idx].FindMissing(ctx, digests)
			missingPerBackend[idx] = missing
		}(idx, builderPerBackend.Build())
	}
	wg.Wait()
	globalMissing := digest.GetUnion(missingPerBackend)
	// upstream found digests
	found, _, _ := digest.GetDifferenceAndIntersection(digests, globalMissing)
	for _, dgst := range found.Items() {
		h := dgst.GetHashString()
		if !justbuild.IsJustbuildTree(h) || emptyblobs.IsEmptyBlob(h) {
			// if it is a simple blob, it has already been upstreamed
			// if it is an empty blob, do nothing as well
			continue
		}
		go m.traverse(h, dgst)
	}
	return globalMissing, nil
}

func (ba *ShardedMultiGenerationBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return nil, nil
}
