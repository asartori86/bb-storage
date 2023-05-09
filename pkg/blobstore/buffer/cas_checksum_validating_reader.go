package buffer

import (
	"bytes"
	"hash"
	"io"
	"math"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

type casChecksumValidatingReader struct {
	io.ReadCloser
	digest digest.Digest
	source Source

	err    error
	hasher hash.Hash
}

// newCASValidatingReader creates a decorator for io.ReadCloser that
// performs on-the-fly checksum validation of the contents as required
// by the Content Addressable Storage. It has been implemented in such a
// way that it does not allow access to the full stream's contents in
// case of size or checksum mismatches.
func newCASChecksumValidatingReader(r io.ReadCloser, digest digest.Digest, source Source) io.ReadCloser {
	return &casChecksumValidatingReader{
		ReadCloser: r,
		digest:     digest,
		source:     source,

		hasher: digest.NewHasher(math.MaxInt64),
	}
}

func (r *casChecksumValidatingReader) compareChecksum() error {
	expectedChecksum := r.digest.GetHashBytes()
	actualChecksum := r.hasher.Sum(nil)
	if bytes.Compare(expectedChecksum, actualChecksum) != 0 {
		return r.source.notifyCASHashMismatch(expectedChecksum, actualChecksum)
	}
	return nil
}

func (r *casChecksumValidatingReader) doRead(p []byte) (int, error) {
	n, readErr := r.ReadCloser.Read(p)
	r.hasher.Write(p[:n])
	if readErr == io.EOF {
		// Compare the blob's checksum.
		if err := r.compareChecksum(); err != nil {
			return 0, err
		}
		r.source.notifyDataValid()
		return n, io.EOF
	} else if readErr != nil {
		return 0, readErr
	}

	return n, nil
}

func (r *casChecksumValidatingReader) Read(p []byte) (int, error) {
	// Return errors from previous iterations. This prevents
	// resumption of I/O after yielding a data integrity error once.
	if r.err != nil {
		return 0, r.err
	}
	n, err := r.doRead(p)
	r.err = err
	return n, err
}
