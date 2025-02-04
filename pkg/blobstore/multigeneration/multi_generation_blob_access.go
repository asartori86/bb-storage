package multigeneration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	emptyblobs "github.com/buildbarn/bb-storage/pkg/empty_blobs"
	"github.com/buildbarn/bb-storage/pkg/justbuild"
	mg_proto "github.com/buildbarn/bb-storage/pkg/proto/multigeneration"
	"github.com/buildbarn/bb-storage/pkg/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/emptypb"
)

type toBeCopied struct {
	idx  uint32
	dgst digest.Digest
}

var MultiGenerationBlobAccessPtr *multiGenerationBlobAccess

type multiGenerationBlobAccess struct {
	capabilities.Provider

	minimumRotationSizeBytes        uint64
	timeIntervalBetweenComputeSizes uint64
	indexes                         []uint32
	rotateLock                      sync.RWMutex
	statusLock                      sync.RWMutex
	generations                     []*singleGeneration
	status                          mg_proto.MultiGenStatus_Value
	lastRotationTimeStamp           int64
	crew                            []blobstore.BlobAccess
	nShards                         uint32
}

func NewMultiGenerationBlobAccess(nGenerations uint32, rotationSizeBytes uint64, timeInterval uint64,
	rootDir string, nShardsSingleGen uint32, maxBlobsPerShard uint32, crew []blobstore.BlobAccess, capabilitiesProvider capabilities.Provider) *multiGenerationBlobAccess {
	if nGenerations <= 1 {
		log.Panicf("ERROR: multiGenerationBlobAccess requires generations > 1 but got %d", nGenerations)
	}

	if nShardsSingleGen < 1 {
		log.Panicf("ERROR: multiGenerationBlobAccess requires n_shards_single_generation > 0 but got %d", nShardsSingleGen)
	}

	var indexes = make([]uint32, nGenerations)
	var generations = make([]*singleGeneration, nGenerations)
	var timeStamps = make([]int64, nGenerations)
	now := time.Now().Unix()
	interval := time.Duration(timeInterval * uint64(time.Second))
	for i := uint32(0); i < nGenerations; i++ {
		indexes[i] = i
		genDir := filepath.Join(rootDir, fmt.Sprintf("gen-%d", i))
		generations[i] = newSingleGeneration(genDir, i, nShardsSingleGen, maxBlobsPerShard, now, interval)
		timeStamps[i] = generations[i].lastCleanUpTimeStamp
	}

	// In case the storage pod was restarted or updated and the previous
	// persistent volume claim is still present, we find the previous current
	// generation
	idxCurrentGen := 0
	currentTimeStamp := now * 0
	for i, t := range timeStamps {
		if t > currentTimeStamp {
			currentTimeStamp = t
			idxCurrentGen = i
		}
	}
	for indexes[0] != uint32(idxCurrentGen) {
		indexes = append(indexes[nGenerations-1:], indexes[0:nGenerations-1]...)
	}

	ba := multiGenerationBlobAccess{
		Provider:                        capabilitiesProvider,
		nShards:                         uint32(len(crew)),
		minimumRotationSizeBytes:        rotationSizeBytes,
		timeIntervalBetweenComputeSizes: timeInterval,
		indexes:                         indexes,
		rotateLock:                      sync.RWMutex{},
		statusLock:                      sync.RWMutex{},
		generations:                     generations,
		status:                          mg_proto.MultiGenStatus_OK,
		lastRotationTimeStamp:           now,
		crew:                            crew,
	}

	// spawn goroutine that will periodically check the size of the current generation
	// if the size is above the given threshold generations will rotate
	go func() {
		tick := time.NewTicker(interval)
		for {
			<-tick.C
			ba.maybeRotate()
		}
	}()
	MultiGenerationBlobAccessPtr = &ba
	MultiGenerationBlobAccessPtr.muninLog()
	return &ba
}

func (ba *multiGenerationBlobAccess) indexToBeDeleted() uint32 {
	n := len(ba.indexes)
	return ba.indexes[n-1]
}

func (ba *multiGenerationBlobAccess) currentIndex() uint32 {
	return ba.indexes[0]
}

func (ba *multiGenerationBlobAccess) getFromGen(hash string, gen uint32) ([]byte, uint32) {
	//assumption: rotate cannot happen concurrently
	if ba.generations[gen].has(hash) {
		data, err := ba.generations[gen].get(hash)
		if err == nil && data != nil {
			return data, gen
		}
		if err != nil {
			log.Printf("blob %s not found in generation %d and got error %s, looking into other generations\n", hash, gen, err)
		}
	}
	// it might be that generations rotated while uploading a tree
	// so, part of it can be in a different generation
	for _, i := range ba.indexes {
		if ba.generations[i].has(hash) {
			data, err := ba.generations[i].get(hash)
			if err == nil && data != nil {
				return data, i
			}
		}
	}
	return nil, 0
}

func (ba *multiGenerationBlobAccess) Get(ctx context.Context, dgst digest.Digest) buffer.Buffer {
	hash := dgst.GetHashString()
	if emptyblobs.IsEmptyBlob(hash) {
		return buffer.NewValidatedBufferFromByteSlice(nil)
	}
	ba.rotateLock.RLock()
	defer ba.rotateLock.RUnlock()
	currentIdx := ba.currentIndex()
	for _, i := range ba.indexes {
		if ba.generations[i].has(hash) {
			dat, gen := ba.getFromGen(hash, i)
			if dat == nil {
				err := fmt.Errorf("%s could not be retrieved from cas: has_gen %d, got nil from %d.", dgst.String(), i, gen)
				log.Printf(err.Error())
				return buffer.NewBufferFromError(err)
			}
			if justbuild.IsJustbuildTree(hash) {
				// we check the completeness of a tree to detect any
				// inconsistencies in the current generation
				err := ba.checkCompleteness(ctx, dgst, dat)
				if err != nil {
					var incompleteTreeError *IncompleteTree
					if errors.As(err, &incompleteTreeError) {
						ba.statusLock.Lock()
						defer ba.statusLock.Unlock()
						ba.status = mg_proto.MultiGenStatus_RESET_NEEDED
						err = util.StatusWrapf(err, "CAS will be reset due to missing trees. Please try again.")
						log.Printf(err.Error())
						return buffer.NewBufferFromError(err)
					}
					return buffer.NewBufferFromError(err)
				}
			}
			if gen != currentIdx {
				ba.generations[currentIdx].uplink(hash, ba.generations[gen].dir)
			}
			return buffer.NewValidatedBufferFromByteSlice(dat)
		}
	}
	err := fmt.Errorf("%s could not be retrieved from CAS.", dgst.String())
	log.Printf(err.Error())
	return buffer.NewBufferFromError(err)
}

func (ba *multiGenerationBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	parentHash := parentDigest.GetHashString()
	childHash := childDigest.GetHashString()
	log.Printf("COMPOSITE: parent=%s   child=%s\n", parentHash, childHash)
	// check if parent is available
	missing, err := ba.FindMissing(ctx, parentDigest.ToSingletonSet().RemoveEmptyBlob())
	if err != nil {
		return buffer.NewBufferFromError(err)
	}
	if !missing.Empty() {
		buffer.NewBufferFromError(fmt.Errorf("Parent digest %#v not found in CAS", parentDigest))
	}
	return ba.Get(ctx, childDigest)
}

func DirectDependencySet(bytes []byte, dgst digest.Digest) (digest.Set, error) {
	hashes, _, _, err := justbuild.GetTaggedHashes(bytes)
	if err != nil {
		log.Printf("Failed to compute tagged hashes for tree %#v", dgst)
		return digest.EmptySet, err
	}
	setBuilder := digest.NewSetBuilder()
	instName := dgst.GetInstanceName().String()
	for _, h := range hashes {
		curDgst := digest.MustNewDigest(instName, dgst.GetDigestFunction().GetEnumValue(), h, 0 /*sizeBytes*/)
		setBuilder.Add(curDgst)
	}
	return setBuilder.Build(), nil
}

type IncompleteTree struct {
	msg string
}

func (x *IncompleteTree) Error() string {
	return x.msg
}

func (ba *multiGenerationBlobAccess) checkCompleteness(ctx context.Context, dgst digest.Digest, bytes []byte) error {
	// make sure all leaves are present in the current generation
	// if at least one leaf is missing it will error out
	leaves, err := DirectDependencySet(bytes, dgst)
	if err != nil {
		return err
	}
	// partition digests by shard
	digestsPerBackend := make([]digest.SetBuilder, 0, ba.nShards)
	for range ba.crew {

		digestsPerBackend = append(digestsPerBackend, digest.NewSetBuilder())
	}
	for _, blobDigest := range leaves.Items() {
		i := FNV(blobDigest.GetHashString(), ba.nShards)
		digestsPerBackend[i].Add(blobDigest)
	}

	missingPerBackend := make([]digest.Set, 0, ba.nShards)

	// asynchronously call FindMissing() on the shards.
	// each FindMissing guarantees that if the digest is present, it is also
	// already in the youngest generation
	group, ctxWithCancel := errgroup.WithContext(ctx)
	for idxIter, digestsIter := range digestsPerBackend {
		// need local variables to be passed to the go subroutine
		idx, digests := idxIter, digestsIter
		if digests.Length() > 0 {
			missingPerBackend = append(missingPerBackend, digest.EmptySet)
			missingOut := &missingPerBackend[len(missingPerBackend)-1]
			group.Go(func() error {
				missing, err := ba.crew[idx].FindMissing(ctxWithCancel, digests.Build())

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
		return err
	}

	missing := digest.GetUnion(missingPerBackend)

	if !missing.Empty() {
		return &IncompleteTree{msg: fmt.Sprintf("Incomplete tree detected %#v: missing leaves are: %#v", dgst, missing)}
	}
	return nil
}

func (ba *multiGenerationBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	hash := digest.GetHashString()

	if emptyblobs.IsEmptyBlob(hash) {
		return nil
	}
	ba.rotateLock.RLock()
	defer ba.rotateLock.RUnlock()

	idx := ba.currentIndex()

	// simple blob
	if !justbuild.IsJustbuildTree(hash) {
		return ba.generations[idx].put(ctx, digest, b)
	}

	// check if all leaves have been uploaded first
	//
	// need to duplicate the buffer because:
	// - one will be consumed to find the leaves
	// - the second one is consumed to store it into the CAS
	s, err := b.GetSizeBytes()
	if err != nil {
		return err
	}
	b1, b2 := b.CloneCopy(int(s))

	bytes, err := b1.ToByteSlice(int(s))
	if err != nil {
		return err
	}

	err = ba.checkCompleteness(ctx, digest, bytes)
	if err != nil {
		return err
	}

	return ba.generations[idx].put(ctx, digest, b2)
}

func (ba *multiGenerationBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	currentDigests := digests
	found := []toBeCopied{}
	ba.rotateLock.RLock()
	defer ba.rotateLock.RUnlock()
	for _, i := range ba.indexes {
		if currentDigests.Empty() {
			break
		}
		missing, up := ba.generations[i].findMissing(currentDigests)
		currentDigests = missing
		found = append(found, up...)
	}

	incomplete := digest.NewSetBuilder()
	currentIdx := ba.currentIndex()
	for _, x := range found {
		if x.idx != currentIdx {
			if justbuild.IsJustbuildTree(x.dgst.GetHashString()) {
				data, _ := ba.getFromGen(x.dgst.GetHashString(), x.idx)
				// Check the completeness to uplink the recursively referenced
				// objects. If the tree is incomplete, we tell the client to
				// re-upload the whole tree, marking the whole tree as missing.
				err := ba.checkCompleteness(ctx, x.dgst, data)
				if err != nil {
					log.Printf("While checking completeness of %v: ", x.dgst.String(), err)
					incomplete.Add(x.dgst)
					continue
				}
			}
			ba.generations[currentIdx].uplink(x.dgst.GetHashString(), ba.generations[x.idx].dir)
		}
	}
	union := []digest.Set{}
	union = append(union, currentDigests, incomplete.Build())
	return digest.GetUnion(union), nil
}

// right rotate indexes
func (ba *multiGenerationBlobAccess) rotate() {
	n := len(ba.indexes)
	ba.indexes = append(ba.indexes[n-1:], ba.indexes[:n-1]...)
	ba.lastRotationTimeStamp = time.Now().Unix()
	log.Printf("rotated indexes %v\n", ba.indexes)
}

func prettyPrintSize(size uint64) string {
	fsize := float64(size)
	var x float64
	var label string
	if x = fsize / (math.Pow(10, 15)); x >= 1.0 {
		label = "PB"
	} else if x = fsize / (math.Pow(10, 12)); x >= 1.0 {
		label = "TB"
	} else if x = fsize / (math.Pow(10, 9)); x >= 1.0 {
		label = "GB"
	} else if x = fsize / (math.Pow(10, 6)); x >= 1.0 {
		label = "MB"
	} else if x = fsize / (math.Pow(10, 3)); x >= 1.0 {
		label = "kB"
	} else {
		x = fsize
		label = "B"
	}
	return fmt.Sprintf("%.2f %s", x, label)
}

type muninData struct {
	CurGenSizeBytes uint64
	CurGenIdx       uint32
	TimeStamps      []int64
}

func (ba *multiGenerationBlobAccess) muninLog() {
	currentIdx := ba.currentIndex()
	ba.generations[currentIdx].mutex.RLock()
	size := ba.generations[currentIdx].curSize
	ba.generations[currentIdx].mutex.RUnlock()
	nGens := len(ba.indexes)
	var timeStamps = make([]int64, nGens)
	for _, i := range ba.indexes {
		ba.generations[i].mutex.RLock()
		timeStamps[i] = ba.generations[i].lastCleanUpTimeStamp
		ba.generations[i].mutex.RUnlock()
	}
	data := muninData{
		CurGenSizeBytes: size,
		CurGenIdx:       currentIdx,
		TimeStamps:      timeStamps,
	}
	marshaled, err := json.Marshal(data)
	if err == nil {
		log.Printf("[munin]:%s", string(marshaled))
	} else {
		log.Println(err)
	}
}

func (ba *multiGenerationBlobAccess) maybeRotate() {
	ba.rotateLock.RLock()
	defer ba.rotateLock.RUnlock()
	currentIdx := ba.currentIndex()
	size := ba.generations[currentIdx].size()
	if size >= ba.minimumRotationSizeBytes {
		ba.statusLock.Lock()
		defer ba.statusLock.Unlock()
		// just set a flag. the controller will handle it
		ba.status = mg_proto.MultiGenStatus_ROTATION_NEEDED
	}
	log.Printf("%s --> %s [threshold = %s], status = %#v\n", ba.generations[currentIdx].dir, prettyPrintSize(size), prettyPrintSize(ba.minimumRotationSizeBytes), ba.status)
	ba.muninLog()
}

// implement the ShardedMultiGenerationControllerServer interface

func (c *multiGenerationBlobAccess) GetStatus(ctx context.Context, in *emptypb.Empty) (*mg_proto.MultiGenReply, error) {
	// log.Printf("got request for GetIfWantsToRotate\n")
	c.statusLock.RLock()
	defer c.statusLock.RUnlock()
	return &mg_proto.MultiGenReply{Status: c.status}, nil
}

func (c *multiGenerationBlobAccess) AcquireRotateLock(ctx context.Context, in *emptypb.Empty) (*mg_proto.MultiGenReply, error) {
	log.Printf("call acquire rotate lock")
	ok := c.rotateLock.TryLock()
	if ok {
		log.Printf("call acquire rotate lock---acquired")
		return &mg_proto.MultiGenReply{Status: mg_proto.MultiGenStatus_OK}, nil
	}
	return &mg_proto.MultiGenReply{Status: mg_proto.MultiGenStatus_INTERNAL_ERROR}, nil
}

func (c *multiGenerationBlobAccess) ReleaseRotateLock(ctx context.Context, in *emptypb.Empty) (*mg_proto.MultiGenReply, error) {
	log.Printf("call release rotate lock")
	c.rotateLock.Unlock()
	log.Printf("call release rotate lock---released")
	return &mg_proto.MultiGenReply{Status: mg_proto.MultiGenStatus_OK}, nil
}

func (c *multiGenerationBlobAccess) DoRotate(ctx context.Context, in *emptypb.Empty) (*mg_proto.MultiGenReply, error) {
	i := c.indexToBeDeleted()
	c.rotate()
	defer c.rotateLock.Unlock()
	err := c.generations[i].reset()
	if err != nil {
		return &mg_proto.MultiGenReply{Status: mg_proto.MultiGenStatus_INTERNAL_ERROR}, err
	}
	c.muninLog()
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	c.status = mg_proto.MultiGenStatus_OK
	return &mg_proto.MultiGenReply{Status: c.status}, nil
}

func (c *multiGenerationBlobAccess) DoReset(ctx context.Context, in *emptypb.Empty) (*mg_proto.MultiGenReply, error) {
	log.Printf("Resetting all generations")
	n := uint32(len(c.indexes))
	for i := uint32(0); i < n; i++ {
		// also reset the indexes such that all storage pods restart from the same generation number
		c.indexes[i] = i
		err := c.generations[i].reset()
		if err != nil {
			return &mg_proto.MultiGenReply{Status: mg_proto.MultiGenStatus_INTERNAL_ERROR}, err
		}
	}
	c.muninLog()
	defer c.rotateLock.Unlock()
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	c.status = mg_proto.MultiGenStatus_OK
	return &mg_proto.MultiGenReply{Status: c.status}, nil
}
