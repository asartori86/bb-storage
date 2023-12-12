package grpcservers

import (
	"io"
	"math/rand"
)

type BlobChunker struct {
	minChunkSize     int
	averageChunkSize int
	maxChunkSize     int
	stream           io.ReadCloser
	dataBuffer       []byte
	size             int
	pos              int
	err              error
}

const (
	DefaultChunkSize int = 1024 * 128
	// Mask values taken from algorithm 2 of the paper
	// https://ieeexplore.ieee.org/document/9055082.
	maskS uint64 = 0x4444d9f003530000 // 19 '1' bits
	maskL uint64 = 0x4444d90003530000 // 14 '1' bits
)

var (
	// Predefined array of 256 random 64-bit integers, needs to be initialized.
	gearTable [256]uint64
)

func init() {
	rnd := rand.New(rand.NewSource(0))
	for i := range gearTable {
		gearTable[i] = rnd.Uint64()
	}
}

func NewBlobChunker(stream io.ReadCloser, averageChunkSize int) *BlobChunker {
	minChunkSize := averageChunkSize >> 2
	maxChunkSize := averageChunkSize << 3
	bufferSize := maxChunkSize << 4
	dataBuffer := make([]byte, bufferSize)
	return &BlobChunker{minChunkSize, averageChunkSize, maxChunkSize, stream, dataBuffer, 0, 0, nil}
}

func (c *BlobChunker) NextChunk() ([]byte, error) {
	if c.err != nil && c.err != io.EOF {
		return nil, c.err
	}

	// Ensure that at least maxChunkSize bytes are in the buffer, except if
	// end-of-file is reached.
	remaining := c.size - c.pos
	if remaining < c.maxChunkSize && c.err != io.EOF {
		// Move the remaining bytes of the buffer to the front.
		copy(c.dataBuffer, c.dataBuffer[c.pos:c.size])
		// Fill the buffer with stream content.
		cnt, err := c.stream.Read(c.dataBuffer[remaining:])
		if err != nil && err != io.EOF {
			return nil, err
		}
		c.err = err
		c.size = cnt + remaining
		c.pos = 0
	}

	// Handle finished chunking.
	if c.pos == c.size {
		return nil, io.EOF
	}

	off := c.nextChunkBoundary()
	newPos := c.pos + off
	chunk := c.dataBuffer[c.pos:newPos]
	c.pos = newPos
	return chunk, nil
}

// Implementation of the FastCDC data deduplication algorithm described in
// algorithm 2 of the paper https://ieeexplore.ieee.org/document/9055082.
func (c *BlobChunker) nextChunkBoundary() int {
	var fp uint64 = 0
	n := c.size - c.pos
	i := c.minChunkSize
	normalSize := c.averageChunkSize
	if n <= c.minChunkSize {
		return n
	}
	if n >= c.maxChunkSize {
		n = c.maxChunkSize
	} else if n <= normalSize {
		normalSize = n
	}
	for ; i < normalSize; i++ {
		fp = (fp << 1) + gearTable[c.dataBuffer[c.pos+i]]
		if (fp & maskS) == 0 {
			return i // if the masked bits are all '0'
		}
	}
	for ; i < n; i++ {
		fp = (fp << 1) + gearTable[c.dataBuffer[c.pos+i]]
		if (fp & maskL) == 0 {
			return i // if the masked bits are all '0'
		}
	}
	return i
}
