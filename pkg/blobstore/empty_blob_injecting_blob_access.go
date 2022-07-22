package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type emptyBlobInjectingBlobAccess struct {
	BlobAccess
}

// NewEmptyBlobInjectingBlobAccess is a decorator for BlobAccess that
// causes it to directly process any requests for blobs of size zero.
// Get() operations immediately return an empty buffer, while Put()
// operations for such buffers are ignored.
//
// Bazel never attempts to read the empty blob from the Content
// Addressable Storage, which by itself is harmless. In addition to
// that, it never attempts to write the empty blob. This is problematic,
// as it may cause unaware implementations of GetActionResult() and
// input root population to fail.
//
// This problem remained undetected for a long time, because running at
// least one build action through bb_worker has a high probability of
// creating the empty blob in storage explicitly.
//
// The consensus within the Remote APIs working group has been to give
// the empty blob a special meaning: the system must behave as if this
// blob is always present.
//
// More details: https://github.com/bazelbuild/bazel/issues/11063
func NewEmptyBlobInjectingBlobAccess(base BlobAccess) BlobAccess {
	return &emptyBlobInjectingBlobAccess{
		BlobAccess: base,
	}
}

var EmptyBlobs map[string]bool

func init() {
	EmptyBlobs = make(map[string]bool)
	// just empty blob
	EmptyBlobs["62e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"] = true
	// just empty tree
	EmptyBlobs["744b825dc642cb6eb9a060e54bf8d69288fbee4904"] = true
	// sha256 empty string
	EmptyBlobs["e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"] = true
	// sha1 empty string
	EmptyBlobs["da39a3ee5e6b4b0d3255bfef95601890afd80709"] = true

}

func isEmptyBlob(hash string) bool {
	return EmptyBlobs[hash]
}

func (ba *emptyBlobInjectingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	hash := digest.GetHashString()
	if isEmptyBlob(hash) {
		return buffer.NewCASBufferFromByteSlice(digest, nil, buffer.UserProvided)
	}
	return ba.BlobAccess.Get(ctx, digest)
}

func (ba *emptyBlobInjectingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	if digest.GetSizeBytes() == 0 {
		_, err := b.ToByteSlice(0)
		return err
	}
	return ba.BlobAccess.Put(ctx, digest, b)
}

func (ba *emptyBlobInjectingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return ba.BlobAccess.FindMissing(ctx, digests.RemoveEmptyBlob())
}
