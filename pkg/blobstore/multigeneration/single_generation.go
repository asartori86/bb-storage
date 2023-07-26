package multigeneration

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	emptyblobs "github.com/buildbarn/bb-storage/pkg/empty_blobs"
)

type shard struct {
	cache  map[string]bool
	rwLock sync.RWMutex
}

func newShard() *shard {
	return &shard{
		cache:  map[string]bool{},
		rwLock: sync.RWMutex{},
	}
}

func (s *shard) has(h string) bool {
	s.rwLock.RLock()
	b := s.cache[h]
	s.rwLock.RUnlock()
	return b
}

func (s *shard) add(h string) {
	s.rwLock.Lock()
	s.cache[h] = true
	s.rwLock.Unlock()
}

type singleGeneration struct {
	dir                  string
	idx                  uint32
	nShards              uint32
	shards               []*shard
	mutex                sync.RWMutex
	lastCleanUpTimeStamp int64
	curSize              uint64
}

func newSingleGeneration(root string, i uint32, nShards uint32, timeStamp int64) *singleGeneration {
	x := singleGeneration{
		dir:                  root,
		idx:                  i,
		nShards:              nShards,
		shards:               make([]*shard, nShards),
		mutex:                sync.RWMutex{},
		lastCleanUpTimeStamp: timeStamp,
		curSize:              0,
	}
	x.initShards()
	// sanity check that we have access to the directory we will handle
	_, err := os.ReadDir(root)
	if err != nil {
		log.Panicf("Unable to access directory %s", root)
	}
	return &x
}

func (c *singleGeneration) initShards() {
	for i := uint32(0); i < c.nShards; i++ {
		c.shards[i] = newShard()
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

func (c *singleGeneration) shardIdx(key string) uint32 {
	return FNV(key, c.nShards)
}

func (c *singleGeneration) has(h string) bool {
	i := c.shardIdx(h)
	return c.shards[i].has(h)
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
	name := filepath.Join(c.dir, hash)
	c.mutex.Lock()
	err = os.WriteFile(name, slice, 0644)
	c.mutex.Unlock()
	if err != nil {
		return err
	}
	c.mutex.RLock()
	data, err := os.ReadFile(name)
	c.mutex.RUnlock()
	if err != nil {
		return err
	}
	hasher := digest.NewHasher(size)
	hasher.Write(data)
	sum := hasher.Sum(nil)
	expectedHash := digest.GetHashBytes()
	if bytes.Compare(expectedHash, sum) != 0 {
		return fmt.Errorf("failed to store blob %s: buffer has checksum %s, while %s was expected",
			digest,
			hex.EncodeToString(sum),
			hex.EncodeToString(expectedHash))
	}
	i := c.shardIdx(hash)
	c.shards[i].add(hash)
	return nil
}

// useful when we recover from disk or uplinking
func (c *singleGeneration) addToCache(hash string) {
	i := c.shardIdx(hash)
	c.shards[i].add(hash)
}

func (c *singleGeneration) uplink(h string, oldDir string) {
	dst := filepath.Join(c.dir, h)
	src := filepath.Join(oldDir, h)
	c.mutex.Lock()
	os.Link(src, dst)
	c.mutex.Unlock()
	if _, err := os.Stat(dst); err == nil {
		c.addToCache(h)
	}
}

func (c *singleGeneration) reset() {
	c.mutex.Lock()
	c.initShards()
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		log.Panicf("Unable to access directory %s", c.dir)
	}
	for _, f := range entries {
		name := filepath.Join(c.dir, f.Name())
		os.RemoveAll(name)
	}
	c.lastCleanUpTimeStamp = time.Now().Unix()
	c.mutex.Unlock()
}

func (c *singleGeneration) get(hash string) ([]byte, error) {
	name := filepath.Join(c.dir, hash)
	c.mutex.RLock()
	data, err := os.ReadFile(name)
	c.mutex.RUnlock()
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
			if i := c.shardIdx(h); c.shards[i].has(h) {
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
	c.mutex.RLock()
	entries, err := os.ReadDir(c.dir)
	c.mutex.RUnlock()
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.curSize = uint64(size)
	return c.curSize
}
