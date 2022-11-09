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
		go m.traverse(hash, digest)
	}
	return b
}

func ToDigestSet(b buffer.Buffer, dgst digest.Digest) (digest.Set, error) {
	size, errSize := b.GetSizeBytes()
	if errSize != nil {
		return digest.EmptySet, errSize
	}
	bytes, errSlice := b.ToByteSlice(int(size))
	if errSlice != nil {
		return digest.EmptySet, errSlice
	}
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
	err := m.backends[i].Put(ctx, digest, b)
	if err == nil {
		if justbuild.IsJustbuildTree(hash) {
			// The client first uploads the leaves and then the tree blob, so, we
			// trigger upstreaming of the whole tree to make sure that each
			// generation honors the invariant.
			m.FindMissing(context.TODO(), digest.ToSingletonSet())
		}
	}
	return err
}

func (m *shardedMultiGenerationBlobAccess) traverse(treeHash string, dgst digest.Digest) {
	m.semaphore <- struct{}{}
	defer func() {
		<-m.semaphore
	}()
	b := m.backends[FNV(treeHash, m.nShards)].Get(context.TODO(), dgst)
	size, errSize := b.GetSizeBytes()
	if errSize != nil {
		log.Printf("err getSizeBytes %s %s\n\n", errSize, dgst)
	} else {
		bytes, errSlice := b.ToByteSlice(int(size))
		if errSlice != nil {
			log.Printf("err toByteSlice %s %s\n\n", errSlice, dgst)
		} else {
			hashes, _, _, _ := justbuild.GetAllHashes(bytes)
			setBuilder := digest.NewSetBuilder()
			instName := dgst.GetInstanceName().String()
			for _, h := range hashes {
				curDgst := digest.MustNewDigest(instName, h, 0 /*sizeBytes*/)
				setBuilder.Add(curDgst)
			}
			missing, _ := m.FindMissing(context.TODO(), setBuilder.Build())
			if missing.Length() > 0 {
				log.Printf("incomplete tree detected %s: missing blobs are %v\n", dgst.GetHashString(), missing)
			}
		}
	}
}

func (m *shardedMultiGenerationBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
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

func (ba *shardedMultiGenerationBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return nil, nil
}
