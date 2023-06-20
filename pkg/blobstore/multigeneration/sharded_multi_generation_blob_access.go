package multigeneration

import (
	"context"
	"fmt"
	"log"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	emptyblobs "github.com/buildbarn/bb-storage/pkg/empty_blobs"
	"github.com/buildbarn/bb-storage/pkg/util"
	"golang.org/x/sync/errgroup"
)

type ShardedMultiGenerationBlobAccess struct {
	nShards  uint32
	backends []blobstore.BlobAccess
}

func NewShardedMultiGenerationBlobAccess(backends []blobstore.BlobAccess) *ShardedMultiGenerationBlobAccess {
	x := &ShardedMultiGenerationBlobAccess{
		nShards:  uint32(len(backends)),
		backends: backends,
	}
	return x
}

func (m *ShardedMultiGenerationBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	hash := digest.GetHashString()
	i := FNV(hash, m.nShards)
	b := m.backends[i].Get(ctx, digest)
	return b
}

func (ba *ShardedMultiGenerationBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	parentHash := parentDigest.GetHashString()
	childHash := childDigest.GetHashString()

	log.Printf("COMPOSITE: parent=%s   child=%s\n", parentHash, childHash)

	missing, err := ba.FindMissing(ctx, parentDigest.ToSingletonSet().RemoveEmptyBlob())
	if err != nil {
		return buffer.NewBufferFromError(err)
	}
	if !missing.Empty() {
		return buffer.NewBufferFromError(fmt.Errorf("Parent digest %s not found in CAS", parentDigest))
	}
	return ba.Get(ctx, childDigest)
}

func (m *ShardedMultiGenerationBlobAccess) checkCompleteness(ctx context.Context, digests digest.Set, tree digest.Digest) error {
	missing, err := m.FindMissing(ctx, digests)
	if err != nil {
		return err
	}
	if !missing.Empty() {
		msg := fmt.Sprintf("incorrect tree upload detected. Tree %s misses these direct dependencies %v\n", tree, missing.Items())
		log.Printf(msg)
		return fmt.Errorf(msg)
	}
	return nil
}

func (m *ShardedMultiGenerationBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	hash := digest.GetHashString()
	i := FNV(hash, m.nShards)

	if emptyblobs.IsEmptyBlob(hash) {
		return nil
	}
	return m.backends[i].Put(ctx, digest, b)
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

	// Asynchronously call FindMissing() on the shards.
	group, ctxWithCancel := errgroup.WithContext(ctx)
	for idxIter, digestsIter := range digestsPerBackend {
		// need local variables to be passed to the go subroutine
		idx, digests := idxIter, digestsIter
		if digests.Length() > 0 {
			missingPerBackend = append(missingPerBackend, digest.EmptySet)
			missingOut := &missingPerBackend[len(missingPerBackend)-1]
			group.Go(func() error {
				missing, err := m.backends[idx].FindMissing(ctxWithCancel, digests.Build())
				if err != nil {
					return util.StatusWrapf(err, "Shard %d", idx)
				}
				*missingOut = missing
				return nil
			})
		}
	}

	// Recombine results.
	if err := group.Wait(); err != nil {
		return digest.EmptySet, err
	}

	return digest.GetUnion(missingPerBackend), nil
}

func (ba *ShardedMultiGenerationBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return nil, nil
}
