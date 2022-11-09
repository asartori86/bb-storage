package multigeneration

import (
	"context"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"sync"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	emptyblobs "github.com/buildbarn/bb-storage/pkg/empty_blobs"
	"github.com/buildbarn/bb-storage/pkg/justbuild"
	bb_storage "github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_storage"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	mg_proto "github.com/buildbarn/bb-storage/pkg/proto/multigeneration"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/protobuf/types/known/emptypb"
)

type toBeCopied struct {
	idx  uint32
	hash string
}

type MultiGenerationBlobAccess struct {
	minimumRotationSizeBytes uint64
	timeInterval             uint64
	indexes                  []uint32
	rotateLock               sync.RWMutex
	generations              []*singleGeneration
	autonomous               bool
	semaphore                chan struct{}
	wantsToRotate            bool
	lastRotationTimeStamp    int64
}

func NewMultiGenerationBlobAccessFromConfiguration(conf *bb_storage.ScannableBlobAccessConfiguration) (*MultiGenerationBlobAccess, []auth.Authorizer, error) {
	getAuthorizer, err := auth.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(conf.GetAuthorizer)
	if err != nil {
		return nil, nil, util.StatusWrap(err, "Failed to create Get() authorizer")
	}
	putAuthorizer, err := auth.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(conf.PutAuthorizer)
	if err != nil {
		return nil, nil, util.StatusWrap(err, "Failed to create Put() authorizer")
	}
	findMissingAuthorizer, err := auth.DefaultAuthorizerFactory.NewAuthorizerFromConfiguration(conf.FindMissingAuthorizer)
	if err != nil {
		return nil, nil, util.StatusWrap(err, "Failed to create FindMissing() authorizer")
	}

	switch backend := conf.Backend.Backend.(type) {
	case *pb.BlobAccessConfiguration_MultiGeneration:
		return NewMultiGenerationBlobAccess(backend.MultiGeneration.NGenerations,
				backend.MultiGeneration.MinimumRotationSizeBytes,
				backend.MultiGeneration.RotationIntervalSeconds,
				backend.MultiGeneration.RootDir,
				backend.MultiGeneration.MaxTreeTraversalConcurrency,
				backend.MultiGeneration.NShardsSingleGeneration,
				backend.MultiGeneration.Autonomous),
			[]auth.Authorizer{getAuthorizer, putAuthorizer, findMissingAuthorizer},
			nil

	}
	return nil, nil, nil
}

func NewMultiGenerationBlobAccess(nGenerations uint32, rotationSizeBytes uint64, timeInterval uint64, rootDir string, treeConcurrency uint32, nShards uint32, autonomous bool) *MultiGenerationBlobAccess {
	if nGenerations <= 1 {
		log.Panicf("ERROR: multiGenerationBlobAccess requires generations > 1 but got %d", nGenerations)
	}
	if autonomous {
		if treeConcurrency < 1 {
			log.Panicf("ERROR: multiGenerationBlobAccess requires tree_traversal_concurrency > 0 but got %d", treeConcurrency)
		}
	}
	if nShards < 1 {
		log.Panicf("ERROR: multiGenerationBlobAccess requires n_shards_single_generation > 0 but got %d", nShards)
	}

	var indexes = make([]uint32, nGenerations)
	var generations = make([]*singleGeneration, nGenerations)
	var timeStamps = make([]time.Time, nGenerations)
	var n sync.WaitGroup
	for i := uint32(0); i < nGenerations; i++ {
		n.Add(1)
		go func(i uint32) {
			defer n.Done()
			indexes[i] = i
			generations[i], timeStamps[i] = newSingleGeneration(filepath.Join(rootDir, fmt.Sprintf("gen-%d", i)), i, nShards)
		}(i)
	}
	n.Wait()
	ba := MultiGenerationBlobAccess{
		minimumRotationSizeBytes: rotationSizeBytes,
		timeInterval:             timeInterval,
		indexes:                  indexes,
		rotateLock:               sync.RWMutex{},
		generations:              generations,
		autonomous:               autonomous,
		wantsToRotate:            false,
		lastRotationTimeStamp:    time.Now().Unix(),
	}
	if autonomous {
		ba.semaphore = make(chan struct{}, treeConcurrency)
	}

	// // find most recent directory
	var mostRecentTime time.Time
	idxMostRecent := 0
	for i, t := range timeStamps {
		if t.Unix() > mostRecentTime.Unix() {
			mostRecentTime = t
			idxMostRecent = i
		}
	}
	for {
		if ba.currentIndex() == uint32(idxMostRecent) {
			break
		}
		ba.rotate()
	}

	// spawn goroutine that will periodically check the size of the current generation
	// if the size is above the given threshold generations will rotate
	go func() {
		tick := time.NewTicker(time.Duration(ba.timeInterval * uint64(time.Second)))
		for {
			<-tick.C
			ba.maybeRotate()
		}
	}()
	return &ba
}

func (ba *MultiGenerationBlobAccess) indexToBeDeleted() uint32 {
	n := len(ba.indexes)
	return ba.indexes[n-1]
}

func (ba *MultiGenerationBlobAccess) currentIndex() uint32 {
	return ba.indexes[0]
}

func (ba *MultiGenerationBlobAccess) getFromGen(hash string, gen uint32) ([]byte, uint32) {
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
	log.Panicf("%s should be present in cas but it is missing\n", hash)
	// we will never reach this, since log.Panicf will call panic()
	// but the compiler needs this anyway
	return nil, 0
}

func (ba *MultiGenerationBlobAccess) traverse(treeHash string, gen uint32, wg *sync.WaitGroup) {
	defer wg.Done()
	if !ba.autonomous {
		return
	}
	ba.semaphore <- struct{}{}
	defer func() {
		<-ba.semaphore
	}()
	currentIdx := ba.currentIndex()
	data, g := ba.getFromGen(treeHash, gen)
	hashes, types, _, err := justbuild.GetAllHashes(data)
	if err != nil {
		log.Printf("getAllHashes %s\n\n", err)
		return
	}
	for i, hash := range hashes {
		if emptyblobs.IsEmptyBlob(hash) {
			continue
		}
		if types[i] == justbuild.Tree {
			wg.Add(1)
			go ba.traverse(hash, g, wg)
		}
		if !ba.generations[g].has(hash) {
			for _, g = range ba.indexes {
				if ba.generations[g].has(hash) {
					break
				}
			}
		}
		ba.generations[currentIdx].uplink(hash, ba.generations[g].dir)
	}
}

func (ba *MultiGenerationBlobAccess) upstream(srcs ...toBeCopied) {
	defer ba.rotateLock.RUnlock()
	currentIdx := ba.currentIndex()
	traverseWG := sync.WaitGroup{}
	for _, src := range srcs {
		idx, hash := src.idx, src.hash
		if justbuild.IsJustbuildTree(hash) {
			traverseWG.Add(1)
			go ba.traverse(hash, idx, &traverseWG)
		}
		if idx == currentIdx {
			continue
		}
		ba.generations[currentIdx].uplink(hash, ba.generations[idx].dir)
	}
	traverseWG.Wait()
}

func (ba *MultiGenerationBlobAccess) Get(ctx context.Context, dgst digest.Digest) buffer.Buffer {
	hash := dgst.GetHashString()
	if emptyblobs.IsEmptyBlob(hash) {
		return buffer.NewValidatedBufferFromByteSlice(nil)
	}
	ba.rotateLock.RLock()

	for _, i := range ba.indexes {
		if ba.generations[i].has(hash) {
			dat, gen := ba.getFromGen(hash, i)
			if dat == nil {
				return buffer.NewBufferFromError(fmt.Errorf("%s could not be retrieved from cas: has_gen %d, got nil from %d", dgst.String(), i, gen))
			}
			go ba.upstream(toBeCopied{idx: gen, hash: hash})
			return buffer.NewValidatedBufferFromByteSlice(dat)
		}
	}
	ba.rotateLock.RUnlock()
	return buffer.NewBufferFromError(fmt.Errorf("%s could not be retrieved from cas", dgst.String()))
}

func (ba *MultiGenerationBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	ba.rotateLock.RLock()
	idx := ba.currentIndex()
	err := ba.generations[idx].put(ctx, digest, b)
	if err != nil {
		ba.rotateLock.RUnlock()
		return err
	}
	hash := digest.GetHashString()
	if ba.autonomous && justbuild.IsJustbuildTree(hash) {
		// guarantees that the tree root and all its children are totally
		// contained in one single generation
		ba.upstream(toBeCopied{
			idx:  idx,
			hash: hash,
		})
	}
	ba.rotateLock.RUnlock()
	return nil
}

func (ba *MultiGenerationBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {

	currentDigests := digests
	upstream := []toBeCopied{}
	ba.rotateLock.RLock()
	for _, i := range ba.indexes {
		if currentDigests.Empty() {
			break
		}
		missing, up := ba.generations[i].findMissing(currentDigests)
		currentDigests = missing
		upstream = append(upstream, up...)
	}
	if len(upstream) > 0 {
		go ba.upstream(upstream...)
	} else {
		ba.rotateLock.RUnlock()
	}
	return currentDigests, nil

}

// right rotate indexes
func (ba *MultiGenerationBlobAccess) rotate() {
	n := len(ba.indexes)
	rotated := make([]uint32, n)
	copy(rotated[1:], ba.indexes[:n-1])
	rotated[0] = ba.indexes[n-1]
	ba.indexes = rotated
	ba.lastRotationTimeStamp = time.Now().Unix()
	log.Printf("rotated indexes %v\n", ba.indexes)

}
func (ba *MultiGenerationBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return nil, nil
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

func (ba *MultiGenerationBlobAccess) maybeRotate() {
	currentIdx := ba.currentIndex()

	size := ba.generations[currentIdx].size()
	checkTime := time.Now().Unix()
	log.Printf("%s --> %s  [threshold = %s]\n", ba.generations[currentIdx].dir, prettyPrintSize(size), prettyPrintSize(ba.minimumRotationSizeBytes))
	if size >= ba.minimumRotationSizeBytes {
		ba.rotateLock.Lock()
		defer ba.rotateLock.Unlock()
		// a rotation could have been triggered by another shard before we
		// locked. In this case, we don't set the rotation flag to true,
		// because it would result in a double rotation
		if checkTime > ba.lastRotationTimeStamp {
			if ba.autonomous {
				// we can perform the rotation by our own
				next := ba.indexToBeDeleted()
				ba.generations[next].reset()
				ba.rotate()
			} else {
				// just set a flag. the controller will handle it
				ba.wantsToRotate = true
			}
		}
	}
}

// implement the ShardedMultiGenerationControllerServer interface

func (c *MultiGenerationBlobAccess) GetIfWantsToRotate(ctx context.Context, in *emptypb.Empty) (*mg_proto.MultiGenReply, error) {
	log.Printf("got request for GetIfWantsToRotate\n")
	return &mg_proto.MultiGenReply{Response: c.wantsToRotate}, nil
}

func (c *MultiGenerationBlobAccess) AcquireRotateLock(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	log.Printf("got request for AcquireRotateLock\n")
	c.rotateLock.Lock()
	return &emptypb.Empty{}, nil
}

func (c *MultiGenerationBlobAccess) ReleaseRotateLock(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	log.Printf("got request for ReleaseRotateLock\n")
	c.rotateLock.Unlock()
	return &emptypb.Empty{}, nil
}

func (c *MultiGenerationBlobAccess) DoRotate(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	log.Printf("got request for DoRotate\n")
	defer c.rotateLock.Unlock()
	next := c.indexToBeDeleted()
	c.generations[next].reset()
	c.rotate()
	c.wantsToRotate = false
	return &emptypb.Empty{}, nil
}
