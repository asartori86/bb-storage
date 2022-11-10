package multigeneration

import (
	"context"
	"log"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	emptyblobs "github.com/buildbarn/bb-storage/pkg/empty_blobs"
	"github.com/buildbarn/bb-storage/pkg/justbuild"
)

type shardedMultiGenerationBlobAccess struct {
	nShards   uint32
	backends  []blobstore.BlobAccess
	semaphore chan struct{}
}

func NewShardedMultiGenerationBlobAccess(backends []blobstore.BlobAccess, concurrency uint32) blobstore.BlobAccess {
	return &shardedMultiGenerationBlobAccess{
		nShards:   uint32(len(backends)),
		backends:  backends,
		semaphore: make(chan struct{}, concurrency),
	}
}

func (m *shardedMultiGenerationBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	hash := digest.GetHashString()
	i := FNV(hash, m.nShards)
	b := m.backends[i].Get(ctx, digest)
	if justbuild.IsJustbuildTree(hash) {
		go m.traverse(hash, digest, false /*blocking*/, nil /*recursionWG*/)
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

func (m *shardedMultiGenerationBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	hash := digest.GetHashString()
	i := FNV(hash, m.nShards)

	if justbuild.IsJustbuildTree(hash) {
		size, errSize := b.GetSizeBytes()
		if errSize != nil {
			return errSize
		}
		bytes, errSlice := b.ToByteSlice(int(size))
		if errSlice != nil {
			return errSlice
		}
		// The client first uploads the leaves and then the tree blob, so, we
		// trigger upstreaming of the whole tree to make sure that each
		// generation honors the invariant.

		entries, err := EntriesSet(bytes, digest)
		if err != nil {
			return err
		}

		// wait till all the children are uplinked to the current generation
		m.findMissing(context.TODO(), entries, true /*blocking*/, nil /*recursionWG*/)
		err = m.backends[i].Put(ctx, digest, buffer.NewValidatedBufferFromByteSlice(bytes))

		// since a rotation could have happened between the two previous calls
		// we upstream again, but in the background since it is unlikely that
		// a new rotation will happen soon
		go m.findMissing(context.TODO(), digest.ToSingletonSet(), false /*blocking*/, nil /*recursionWG*/)
		return err
	}

	return m.backends[i].Put(ctx, digest, b)

}

func (m *shardedMultiGenerationBlobAccess) traverse(treeHash string, dgst digest.Digest, blocking bool, wg *sync.WaitGroup) {
	m.semaphore <- struct{}{}
	defer func() {
		<-m.semaphore
		if wg != nil {
			wg.Done()
		}
	}()
	b := m.backends[FNV(treeHash, m.nShards)].Get(context.TODO(), dgst)
	size, _ := b.GetSizeBytes()
	bytes, _ := b.ToByteSlice(int(size))
	entries, _ := EntriesSet(bytes, dgst)
	missing, _ := m.findMissing(context.TODO(), entries, blocking, wg)
	if missing.Length() > 0 {
		log.Printf("incomplete tree detected %s: missing blobs are %v\n", dgst.GetHashString(), missing)
	}
}

func (m *shardedMultiGenerationBlobAccess) findMissing(ctx context.Context, digests digest.Set, blocking bool, recursionWG *sync.WaitGroup) (digest.Set, error) {
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
		callTraverse := func(recursionWG *sync.WaitGroup) { m.traverse(h, dgst, blocking, recursionWG) }
		if blocking {
			isProcessingRootTree := false
			if recursionWG == nil {
				recursionWG = &sync.WaitGroup{}
				isProcessingRootTree = true
			}
			recursionWG.Add(1)
			go callTraverse(recursionWG)

			if isProcessingRootTree {
				recursionWG.Wait()
			}
		} else {
			go callTraverse(nil)
		}
	}
	return globalMissing, nil
}

func (m *shardedMultiGenerationBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return m.findMissing(ctx, digests, false /*blocking*/, nil /*recursionWG*/)
}

func (ba *shardedMultiGenerationBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return nil, nil
}
