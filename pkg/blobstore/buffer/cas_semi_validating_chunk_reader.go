package buffer

import (
	"bytes"
	"hash"
	"io"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

type casSemiValidatingChunkReader struct {
	ChunkReader
	digest digest.Digest
	source Source

	err    error
	hasher hash.Hash
}

// newCASSemiValidatingChunkReader creates a decorator for ChunkReader that
// performs on-the-fly checksum validation of the contents as required
// by the Content Addressable Storage. It is named "Semi" because it does
// not also check the expected size from the digest, which CASValidatingChunkReader
// does. CASSemiValidatingChunkReader can be used when only the hash is known.
// This could happen, for example, while retrieving blobs and trees econded within
// a git tree blob, which does not store the sizes.
func newCASSemiValidatingChunkReader(r ChunkReader, digest digest.Digest, source Source) ChunkReader {
	return &casSemiValidatingChunkReader{
		ChunkReader: r,
		digest:      digest,
		source:      source,

		hasher: digest.NewHasher(),
	}
}

func (r *casSemiValidatingChunkReader) doRead() ([]byte, error) {
	chunk, err := r.ChunkReader.Read()
	r.hasher.Write(chunk)
	if err == io.EOF {
		// validate checksum
		expectedChecksum := r.digest.GetHashBytes()
		actualChecksum := r.hasher.Sum(nil)
		if !bytes.Equal(expectedChecksum, actualChecksum) {
			return nil, r.source.notifyCASHashMismatch(expectedChecksum, actualChecksum)
		}
		r.source.notifyDataValid()
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}

	// keep reading
	return chunk, nil
}

func (r *casSemiValidatingChunkReader) Read() ([]byte, error) {
	// Return errors from previous iterations.
	if r.err != nil {
		return nil, r.err
	}

	// Read the next chunk of data.
	var chunk []byte
	chunk, r.err = r.doRead()
	if r.err != nil {
		return nil, r.err
	}

	return chunk, nil
}
