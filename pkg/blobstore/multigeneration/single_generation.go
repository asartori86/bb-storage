package multigeneration

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	emptyblobs "github.com/buildbarn/bb-storage/pkg/empty_blobs"
)

type Pair struct {
	Key   string
	Value time.Time
}

type shard struct {
	cache   map[string]time.Time
	rwLock  sync.RWMutex
	ticker  *time.Ticker
	done    chan (bool)
	maxSize int
}

func newShard(timeInterval time.Duration, maxBlobs uint32) *shard {
	x := shard{
		cache:   map[string]time.Time{},
		rwLock:  sync.RWMutex{},
		ticker:  time.NewTicker(timeInterval),
		done:    make(chan bool, 1),
		maxSize: int(maxBlobs),
	}
	go func() {
		for {
			select {
			case <-x.done:
				return
			case <-x.ticker.C:
				x.prune()
			}
		}
	}()
	return &x
}

func (s *shard) stopTicker() {
	s.ticker.Stop()
	s.done <- true
}

func (s *shard) prune() {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	delta := len(s.cache) - s.maxSize
	if delta > 0 {
		log.Printf("Pruning cache: number of elements %d exceeds %d", len(s.cache), s.maxSize)
		list := []Pair{}
		for k, v := range s.cache {
			list = append(list, Pair{
				Key:   k,
				Value: v,
			})
		}
		sort.SliceStable(list, func(i, j int) bool {
			return list[i].Value.Before(list[j].Value)
		})
		for x := 0; x < delta; x++ {
			log.Printf("pruning %s [%s]", list[x].Key, list[x].Value)
			delete(s.cache, list[x].Key)
		}
	}
}

func (s *shard) has(h string) bool {
	s.rwLock.RLock()
	_, ok := s.cache[h]
	s.rwLock.RUnlock()
	if ok {
		s.rwLock.Lock()
		defer s.rwLock.Unlock()
		s.cache[h] = time.Now()
		return true
	}
	return false
}

func (s *shard) add(h string) {
	s.rwLock.Lock()
	s.cache[h] = time.Now()
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
	timeInterval         time.Duration
	maxBlobsPerShard     uint32
}

func newSingleGeneration(root string, idx uint32, nShards uint32, maxBlobsPerShard uint32, timeStamp int64, timeInterval time.Duration) (*singleGeneration, int64) {
	// sanity check that we have access to the directory we will handle
	_, err := os.ReadDir(root)
	if err != nil {
		log.Panicf("Unable to access directory %s", root)
	}

	// create all possible directories such that we don't need to check when a
	// blob is added to the cache
	for i := 0; i < 256; i++ {
		dir := filepath.Join(root, fmt.Sprintf("%02x", i))
		err = os.MkdirAll(dir, 0700)
		if err != nil {
			log.Panicf("Unable to create directory %s", dir)
		}
		_, err = os.Stat(dir)
		if err != nil {
			log.Panicf("Unable to access directory %s", dir)
		}
	}

	// compute the timestamp of the oldest blob (or directory) present (if any)
	// to correctly display the cache uptime
	generationTime := timeStamp
	mostRecentBlob := timeStamp * 0
	sizeBytes := 0
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		t := info.ModTime().Unix()
		if t < generationTime {
			generationTime = t
		}
		if t > mostRecentBlob {
			mostRecentBlob = t
		}
		if !info.IsDir() {
			sizeBytes += int(info.Size())
		}
		return err
	})

	if err != nil {
		log.Printf("while traversing %s for computing the oldest time stamp: %#v", root, err)
	}
	x := singleGeneration{
		dir:                  root,
		idx:                  idx,
		nShards:              nShards,
		shards:               make([]*shard, nShards),
		mutex:                sync.RWMutex{},
		lastCleanUpTimeStamp: generationTime,
		curSize:              uint64(sizeBytes),
		timeInterval:         timeInterval,
		maxBlobsPerShard:     maxBlobsPerShard,
	}
	x.initShards(timeInterval, maxBlobsPerShard)
	return &x, mostRecentBlob
}

func (c *singleGeneration) initShards(timeInterval time.Duration, maxBlobs uint32) {
	for i := uint32(0); i < c.nShards; i++ {
		c.shards[i] = newShard(timeInterval, maxBlobs)
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
	i := c.shardIdx(h)
	c.mutex.RLock()
	if c.shards[i].has(h) {
		c.mutex.RUnlock()
		return true
	}
	name := c.blobPath(h)
	_, err := os.Stat(name)
	if err == nil {
		c.addToCache(h)
		c.mutex.RUnlock()
		return true
	}
	c.mutex.RUnlock()
	return false
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
	dst := c.blobPath(h)
	src := c.blobPathDir(oldDir, h)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	os.Link(src, dst)
	if _, err := os.Stat(dst); err == nil {
		c.addToCache(h)
	}
}

func (c *singleGeneration) reset() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, shard := range c.shards {
		shard.stopTicker()
	}
	c.initShards(c.timeInterval, c.maxBlobsPerShard)
	err := filepath.WalkDir(c.dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			os.RemoveAll(path)
		}
		return err
	})
	if err != nil {
		log.Printf("While resetting %s: %#v", c.dir, err)
	}
	c.lastCleanUpTimeStamp = time.Now().Unix()
}

func (c *singleGeneration) get(hash string) ([]byte, error) {
	name := c.blobPath(hash)
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

func computeSize(root string) int64 {
	var sizeBytes int64
	err := filepath.WalkDir(root, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			sizeBytes += info.Size()
		}
		return err
	})
	if err != nil {
		log.Printf("While traversing directory %s: %#v", root, err)
		return 0
	}
	return sizeBytes
}

func (c *singleGeneration) size() uint64 {
	c.mutex.RLock()
	sizeBytes := computeSize(c.dir)
	c.mutex.RUnlock()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.curSize = uint64(sizeBytes)
	return c.curSize
}
