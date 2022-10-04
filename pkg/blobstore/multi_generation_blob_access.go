package blobstore

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	emptyblobs "github.com/buildbarn/bb-storage/pkg/empty_blobs"
	"github.com/buildbarn/bb-storage/pkg/justbuild"
)

type toBeCopied struct {
	idx  uint32
	hash string
}

type singleGeneration struct {
	dir    string
	idx    uint32
	cache  map[string]bool
	rwlock sync.RWMutex
}

func newSingleGeneration(root string, i uint32) (*singleGeneration, time.Time) {
	x := singleGeneration{
		dir:    root,
		idx:    i,
		cache:  map[string]bool{},
		rwlock: sync.RWMutex{},
	}
	// scan dir to recover from disk
	entries, err := os.ReadDir(root)
	if err != nil {
		log.Panicf("Unable to access directory %s", root)
	}
	for _, f := range entries {
		fInfo, err := f.Info()
		if err == nil {
			x.cache[fInfo.Name()] = true
		}
	}
	// return timestamp of root. used to find the most recent generation
	di, _ := os.Stat(root)
	t := di.ModTime()
	return &x, t
}

func (c *singleGeneration) has(h string) bool {
	c.rwlock.RLock()
	defer c.rwlock.RUnlock()
	return c.hasUnguarded(h)
}

func (c *singleGeneration) hasUnguarded(h string) bool {
	return c.cache[h]
}

// used to put a new blob into the directory
func (c *singleGeneration) put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	hash := digest.GetHashString()
	if emptyblobs.IsEmptyBlob(hash) {
		return nil
	}
	size, err := b.GetSizeBytes()
	if err != nil {
		return err
	}

	bytes, err := b.ToByteSlice(int(size))
	if err != nil {
		return err
	}
	name := filepath.Join(c.dir, hash)
	err = os.WriteFile(name, bytes, 0644)
	if err != nil {
		return err
	}

	c.rwlock.Lock()
	defer c.rwlock.Unlock()
	c.cache[hash] = true
	return nil
}

// useful when we recover from disk or uplinking
func (c *singleGeneration) addToCache(h string) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()
	c.cache[h] = true
}

func (c *singleGeneration) uplink(h string, oldDir string) {
	dst := filepath.Join(c.dir, h)
	src := filepath.Join(oldDir, h)
	os.Link(src, dst)
	if _, err := os.Stat(dst); err == nil {
		c.addToCache(h)
	}
}

func (c *singleGeneration) reset() {
	go func(oldestSet map[string]bool, oldestPath string) {
		for dgst := range oldestSet {
			os.RemoveAll(filepath.Join(oldestPath, dgst))
		}

	}(c.cache, c.dir)

	c.rwlock.Lock()
	defer c.rwlock.Unlock()
	c.cache = map[string]bool{}
}

func (c *singleGeneration) get(hash string) ([]byte, error) {
	name := filepath.Join(c.dir, hash)
	data, err := os.ReadFile(name)
	return data, err
}

func (c *singleGeneration) findMissing(digests digest.Set) (digest.Set, []toBeCopied) {
	upstream := []toBeCopied{}
	missing := digest.NewSetBuilder()
	c.rwlock.RLock()
	for _, dgst := range digests.Items() {
		h := dgst.GetHashString()
		if emptyblobs.IsEmptyBlob(h) {
			continue
		}
		if c.hasUnguarded(h) {
			upstream = append(upstream, toBeCopied{hash: h, idx: c.idx})
		} else {
			missing.Add(dgst)
		}
	}
	c.rwlock.RUnlock()
	return missing.Build(), upstream

}

func (c *singleGeneration) size() uint64 {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		log.Panicf("Unable to access directory %s", c.dir)
	}
	var size int64
	size = 0
	for _, f := range entries {
		x, err := f.Info()
		if err == nil {
			size += x.Size()
		}
	}
	return uint64(size)
}

type multiGenerationBlobAccess struct {
	minimumRotationSizeBytes uint64
	TimeInterval             uint64
	indexes                  []uint32
	rotateLock               sync.RWMutex
	generations              []*singleGeneration
}

func NewMultiGenerationBlobAccess(nGenerations uint32, rotationSizeBytes uint64, timeInterval uint64, rootDir string) BlobAccess {
	fmt.Printf("\n\n multi gen\n\n")
	if nGenerations <= 1 {
		log.Panicf("ERROR: multiGenerationBlobAccess requires generations > 1 but got %d", nGenerations)

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
			generations[i], timeStamps[i] = newSingleGeneration(filepath.Join(rootDir, fmt.Sprintf("gen-%d", i)), i)
		}(i)
	}
	n.Wait()
	ba := multiGenerationBlobAccess{
		generations:              generations,
		minimumRotationSizeBytes: rotationSizeBytes,
		TimeInterval:             timeInterval,
		indexes:                  indexes,
		rotateLock:               sync.RWMutex{},
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
	if ba.generations[gen].has(hash) {
		data, err := ba.generations[gen].get(hash)
		if err == nil && data != nil {
			return data, gen
		}
	}
	// it might be that generations rotated while uploading a tree
	// so, part of it could be in a different generation
	for _, i := range ba.indexes {
		if ba.generations[i].has(hash) {
			data, _ := ba.generations[i].get(hash)
			return data, i
		}
	}
	panic(fmt.Errorf("%s should be present in cas but it is missing", hash))
}

func (ba *multiGenerationBlobAccess) traverse(treeHash string, gen uint32, n *sync.WaitGroup) {
	defer n.Done()
	currentIdx := ba.currentIndex()
	data, g := ba.getFromGen(treeHash, gen)
	hashes, types, _, err := justbuild.GetAllHashes(data)
	if err != nil {
		fmt.Printf("\n\ngetAllHashes %s\n\n", err)
	}
	for i, hash := range hashes {
		if emptyblobs.IsEmptyBlob(hash) {
			continue
		}
		if types[i] == justbuild.Tree {
			n.Add(1)
			go ba.traverse(hash, gen, n)
		}
		_, g := ba.getFromGen(hash, g)
		ba.generations[currentIdx].uplink(hash, ba.generations[g].dir)
	}
}

func (ba *multiGenerationBlobAccess) upstream(srcs ...toBeCopied) {
	currentIdx := ba.currentIndex()
	n := sync.WaitGroup{}
	for _, src := range srcs {
		idx, hash := src.idx, src.hash
		if idx == currentIdx {
			continue
		}
		if justbuild.IsJustbuildTree(hash) {
			n.Add(1)
			go ba.traverse(hash, idx, &n)
		}
		ba.generations[currentIdx].uplink(hash, ba.generations[idx].dir)
	}
	n.Wait()
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
			go func() {
				ba.upstream(toBeCopied{idx: gen, hash: hash})
				ba.rotateLock.RUnlock()
			}()
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
	go func() {
		if len(upstream) > 0 {
			ba.upstream(upstream...)
		}
		ba.rotateLock.RUnlock()
	}()
	return currentDigests, nil

}

// right rotate indexes
func (ba *multiGenerationBlobAccess) rotate() {
	n := len(ba.indexes)
	rotated := make([]uint32, n)
	copy(rotated[1:], ba.indexes[:n-1])
	rotated[0] = ba.indexes[n-1]
	ba.indexes = rotated
}
func (ba *multiGenerationBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return nil, nil
}

// check if enough time is passed and check the size of current generation,
// if it is bigger than rotationSizeBytes, it rotates the caches
func (ba *multiGenerationBlobAccess) maybeRotate() {
	currentIdx := ba.currentIndex()

	size := ba.generations[currentIdx].size()
	log.Printf("\n\n %s --> %d bytes\n\n", ba.generations[currentIdx].dir, size)
	if uint64(size) >= ba.minimumRotationSizeBytes {
		ba.rotateLock.Lock()
		next := ba.indexToBeDeleted()
		ba.generations[next].reset()
		ba.rotate()
		ba.rotateLock.Unlock()
		log.Printf("\n\n rotated idx %v\n\n", ba.indexes)
	}
}
