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
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	emptyblobs "github.com/buildbarn/bb-storage/pkg/empty_blobs"
	"github.com/buildbarn/bb-storage/pkg/justbuild"
)

type toBeCopied struct {
	idx  uint32
	hash string
}

type multiGenerationBlobAccess struct {
	minimumRotationSizeBytes uint64
	TimeInterval             uint64
	indexes                  []uint32
	rotateLock               sync.RWMutex
	generations              []*singleGeneration
	treeTraverse             bool
	semaphore                chan struct{}
}

func NewMultiGenerationBlobAccess(nGenerations uint32, rotationSizeBytes uint64, timeInterval uint64, rootDir string, treeConcurrency uint32, nShards uint32, treeTraverse bool) blobstore.BlobAccess {
	if nGenerations <= 1 {
		log.Panicf("ERROR: multiGenerationBlobAccess requires generations > 1 but got %d", nGenerations)
	}
	if treeTraverse {
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
	ba := multiGenerationBlobAccess{
		minimumRotationSizeBytes: rotationSizeBytes,
		TimeInterval:             timeInterval,
		indexes:                  indexes,
		rotateLock:               sync.RWMutex{},
		generations:              generations,
		treeTraverse:             treeTraverse,
	}
	if treeTraverse {
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
		tick := time.NewTicker(time.Duration(ba.TimeInterval * uint64(time.Second)))
		for {
			<-tick.C
			ba.maybeRotate()
		}
	}()

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
	panic(fmt.Errorf("%s should be present in cas but it is missing", hash))
}

func (ba *multiGenerationBlobAccess) traverse(treeHash string, gen uint32, wg *sync.WaitGroup) {
	defer wg.Done()
	if !ba.treeTraverse {
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
		log.Printf("\n\ngetAllHashes %s\n\n", err)
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

func (ba *multiGenerationBlobAccess) upstream(srcs ...toBeCopied) {
	defer ba.rotateLock.RUnlock()
	currentIdx := ba.currentIndex()
	traverseWG := sync.WaitGroup{}
	for _, src := range srcs {
		idx, hash := src.idx, src.hash
		if idx == currentIdx {
			continue
		}
		if justbuild.IsJustbuildTree(hash) {
			traverseWG.Add(1)
			go ba.traverse(hash, idx, &traverseWG)
		}
		ba.generations[currentIdx].uplink(hash, ba.generations[idx].dir)
	}
	traverseWG.Wait()
}

func (ba *multiGenerationBlobAccess) Get(ctx context.Context, dgst digest.Digest) buffer.Buffer {
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

func (ba *multiGenerationBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	ba.rotateLock.RLock()
	defer ba.rotateLock.RUnlock()
	idx := ba.currentIndex()
	return ba.generations[idx].put(ctx, digest, b)
}

func (ba *multiGenerationBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {

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
func (ba *multiGenerationBlobAccess) rotate() {
	n := len(ba.indexes)
	rotated := make([]uint32, n)
	copy(rotated[1:], ba.indexes[:n-1])
	rotated[0] = ba.indexes[n-1]
	ba.indexes = rotated
	log.Printf("\n\n rotated indexes %v\n\n", ba.indexes)

}
func (ba *multiGenerationBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
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

// check if enough time is passed and check the size of current generation,
// if it is bigger than rotationSizeBytes, it rotates the caches
func (ba *multiGenerationBlobAccess) maybeRotate() {
	currentIdx := ba.currentIndex()

	size := ba.generations[currentIdx].size()
	log.Printf("\n\n %s --> %s  [threshold = %s]\n\n", ba.generations[currentIdx].dir, prettyPrintSize(size), prettyPrintSize(ba.minimumRotationSizeBytes))
	if size >= ba.minimumRotationSizeBytes {
		ba.rotateLock.Lock()
		next := ba.indexToBeDeleted()
		ba.generations[next].reset()
		ba.rotate()
		ba.rotateLock.Unlock()
	}
}
