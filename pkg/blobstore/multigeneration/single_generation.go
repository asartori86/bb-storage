package multigeneration

import (
	"context"
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
	dir     string
	idx     uint32
	nShards uint32
	shards  []*shard
}

func newSingleGeneration(root string, i uint32, nShards uint32) (*singleGeneration, time.Time) {
	x := singleGeneration{
		dir:     root,
		idx:     i,
		nShards: nShards,
		shards:  make([]*shard, nShards),
	}
	x.initShards()
	// scan dir to recover from disk
	entries, err := os.ReadDir(root)
	if err != nil {
		log.Panicf("Unable to access directory %s", root)
	}
	for _, f := range entries {
		fInfo, err := f.Info()
		if err == nil {
			name := fInfo.Name()
			i := x.shardIdx(name)
			x.shards[i].add(name)
		}
	}
	// return timestamp of root. used to find the most recent generation
	di, _ := os.Stat(root)
	t := di.ModTime()
	return &x, t
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

	bytes, err := b.ToByteSlice(int(size))
	if err != nil {
		return err
	}
	name := filepath.Join(c.dir, hash)
	err = os.WriteFile(name, bytes, 0644)
	if err != nil {
		return err
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
	os.Link(src, dst)
	if _, err := os.Stat(dst); err == nil {
		c.addToCache(h)
	}
}

func (c *singleGeneration) reset() {
	c.initShards()
	go func() {
		entries, err := os.ReadDir(c.dir)
		if err != nil {
			log.Panicf("Unable to access directory %s", c.dir)
		}
		for _, f := range entries {
			os.RemoveAll(filepath.Join(c.dir, f.Name()))
		}
	}()
}

func (c *singleGeneration) get(hash string) ([]byte, error) {
	name := filepath.Join(c.dir, hash)
	data, err := os.ReadFile(name)
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
				upstreamChnl <- toBeCopied{hash: h, idx: c.idx}
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
