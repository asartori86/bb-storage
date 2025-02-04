package multigeneration

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	emptyblobs "github.com/buildbarn/bb-storage/pkg/empty_blobs"
)

type shard struct {
	cache *lruCache
	lock  sync.Mutex
}

func newShard(maxBlobs uint32) *shard {
	x := shard{
		cache: NewLRUCache(maxBlobs),
		lock:  sync.Mutex{},
	}
	return &x
}

func (s *shard) has(h string) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.cache.Has(h)
}

func (s *shard) add(h string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.cache.Add(h)
}

type singleGeneration struct {
	dir                  string
	idx                  uint32
	nShards              uint32
	shards               []*shard
	mutex                sync.RWMutex
	lastCleanUpTimeStamp int64
	curSize              uint64
	maxBlobsPerShard     uint32
}

func createDirectory(root string) error {
	err := os.MkdirAll(root, 0700)
	if err != nil {
		log.Panicf("Unable to create directory %s", root)
		return err
	}
	// sanity check that we have access to the directory we will handle
	_, err = os.ReadDir(root)
	if err != nil {
		log.Panicf("Unable to access directory %s", root)
		return err
	}

	// create all possible directories such that we don't need to check when a
	// blob is added to the cache
	for i := 0; i < 256; i++ {
		dir := filepath.Join(root, fmt.Sprintf("%02x", i))
		err = os.MkdirAll(dir, 0700)
		if err != nil {
			log.Panicf("Unable to create directory %s", dir)
			return err
		}
		_, err = os.Stat(dir)
		if err != nil {
			log.Panicf("Unable to access directory %s", dir)
			return err
		}
	}
	return nil
}

func readEpoch(root string) (int64, error) {
	epochFile := filepath.Join(root, "epoch")
	_, err := os.Stat(epochFile)
	if err != nil {
		return -1, err
	}
	data, err := os.ReadFile(epochFile)
	if err != nil {
		return -1, err
	}
	t, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return -1, err
	}
	return t, nil
}

func readSize(root string) (uint64, error) {
	epochFile := filepath.Join(root, "size")
	_, err := os.Stat(epochFile)
	if err != nil {
		return 0, err
	}
	data, err := os.ReadFile(epochFile)
	if err != nil {
		return 0, err
	}
	s, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return 0, err
	}
	return s, nil
}

func newSingleGeneration(root string, idx uint32, nShards uint32, maxBlobsPerShard uint32, timeStamp int64, timeInterval time.Duration) *singleGeneration {

	err := createDirectory(root)
	if err != nil {
		return nil
	}

	// compute the timestamp of the youngest blob (or directory) present (if any)
	// to figure it out the youngest generation
	info, err := os.Stat(root)
	if err != nil {
		log.Panicf("Unable to access directory %s", root)
	}
	generationTime := info.ModTime().Unix()

	// check if epoch file is present
	t, err := readEpoch(root)
	if err == nil {
		generationTime = t
	}
	// check if file size is present
	size, _ := readSize(root)
	x := singleGeneration{
		dir:                  root,
		idx:                  idx,
		nShards:              nShards,
		shards:               make([]*shard, nShards),
		mutex:                sync.RWMutex{},
		lastCleanUpTimeStamp: generationTime,
		curSize:              size,
		maxBlobsPerShard:     maxBlobsPerShard,
	}
	x.initShards(maxBlobsPerShard)
	x.dumpEpoch()
	x.dumpSize()
	return &x
}

func (c *singleGeneration) initShards(maxBlobs uint32) {
	for i := uint32(0); i < c.nShards; i++ {
		c.shards[i] = newShard(maxBlobs)
	}
}

const (
	// credits: https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function#FNV_hash_parameters
	offset32 = 2166136261
	prime32  = 16777619
)

func FNV(key string, nShards uint32) uint32 {
	var hash uint32 = offset32
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash % nShards
}

func (c *singleGeneration) blobPathDir(dir, h string) string {
	d := filepath.Join(dir, h[2:4])
	return filepath.Join(d, h)
}

func (c *singleGeneration) blobPath(h string) string {
	return c.blobPathDir(c.dir, h)
}

func (c *singleGeneration) shardIdx(key string) uint32 {
	return FNV(key, c.nShards)
}

func (c *singleGeneration) has(h string) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	i := c.shardIdx(h)
	if c.shards[i].has(h) {
		return true
	}

	name := c.blobPath(h)
	_, err := os.Stat(name)
	if err == nil {
		c.addToCache(h)
		return true
	}
	// to be removed: allow for a smooth transition to blob sharding
	legacyName := filepath.Join(c.dir, h)
	_, err = os.Stat(legacyName)
	if err != nil {
		return false
	}
	dst := c.blobPath(h)
	os.Link(legacyName, dst)
	c.addToCache(h)
	return true
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

	slice, err := b.ToByteSlice(int(size))
	if err != nil {
		return err
	}
	name := c.blobPath(hash)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	err = os.WriteFile(name, slice, 0644)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	hasher := digest.NewHasher(size)
	hasher.Write(data)
	sum := hasher.Sum(nil)
	expectedHash := digest.GetHashBytes()
	if bytes.Compare(expectedHash, sum) != 0 {
		os.Remove(name)
		return fmt.Errorf("failed to store blob %s: buffer has checksum %s, while %s was expected",
			digest,
			hex.EncodeToString(sum),
			hex.EncodeToString(expectedHash))
	}
	c.addToCache(hash)
	c.curSize += uint64(digest.GetSizeBytes())
	return nil
}

// useful when we recover from disk or uplinking
func (c *singleGeneration) addToCache(hash string) {
	i := c.shardIdx(hash)
	c.shards[i].add(hash)
}

func (c *singleGeneration) uplink(h string, oldDir string) {
	dst := c.blobPath(h)
	src := c.blobPathDir(oldDir, h)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	os.Link(src, dst)
	if x, err := os.Stat(dst); err == nil {
		c.addToCache(h)
		c.curSize += uint64(x.Size())
	}
}

func (c *singleGeneration) reset() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.initShards(c.maxBlobsPerShard)
	os.RemoveAll(c.dir)
	err := createDirectory(c.dir)
	if err != nil {
		return err
	}
	c.lastCleanUpTimeStamp = time.Now().Unix()

	c.curSize = 0
	c.dumpEpoch()
	c.dumpSize()
	return nil
}

func (c *singleGeneration) get(hash string) ([]byte, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	name := c.blobPath(hash)
	data, err := os.ReadFile(name)
	c.addToCache(hash)
	return data, err
}

func (c *singleGeneration) findMissing(digests digest.Set) (digest.Set, []toBeCopied) {
	upstream := []toBeCopied{}
	missing := digest.NewSetBuilder()
	upstreamChnl := make(chan toBeCopied, digests.Length())
	missingChnl := make(chan digest.Digest, digests.Length())
	var producersWG sync.WaitGroup
	for _, dgst := range digests.Items() {
		h := dgst.GetHashString()
		if emptyblobs.IsEmptyBlob(h) {
			continue
		}
		producersWG.Add(1)
		go func(dgst digest.Digest, h string) {
			defer producersWG.Done()
			if c.has(h) {
				upstreamChnl <- toBeCopied{dgst: dgst, idx: c.idx}
			} else {
				missingChnl <- dgst
			}
		}(dgst, h)
	}
	// close channles so consumers can exit the loop
	go func() {
		producersWG.Wait()
		close(upstreamChnl)
		close(missingChnl)
	}()
	var consumersWG sync.WaitGroup
	consumersWG.Add(1)
	go func() {
		defer consumersWG.Done()
		for x := range upstreamChnl {
			upstream = append(upstream, x)
		}
	}()
	consumersWG.Add(1)
	go func() {
		defer consumersWG.Done()
		for x := range missingChnl {
			missing.Add(x)
		}
	}()
	consumersWG.Wait()
	return missing.Build(), upstream
}

func (c *singleGeneration) size() uint64 {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.dumpSize()
	return c.curSize
}

func (c *singleGeneration) dumpSize() {
	f, err := os.Create(filepath.Join(c.dir, "size"))
	if err != nil {
		panic(err)
	}
	defer f.Close()
	_, err = f.WriteString(fmt.Sprintf("%d", c.curSize))
	if err != nil {
		panic(err)
	}
}

func (c *singleGeneration) dumpEpoch() {
	f, err := os.Create(filepath.Join(c.dir, "epoch"))
	if err != nil {
		panic(err)
	}
	defer f.Close()
	_, err = f.WriteString(fmt.Sprintf("%d", c.lastCleanUpTimeStamp))
	if err != nil {
		panic(err)
	}
}
