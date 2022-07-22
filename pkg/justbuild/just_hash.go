package justbuild

import (
	"crypto/sha1"
	"fmt"
	"hash"
)

const Size = sha1.Size + 1
const BlockSize = sha1.BlockSize

const BlobMarker = "62"
const TreeMarker = "74"
const MarkerSize = 2 // two runes

type JustHasher struct {
	content []byte
	tree    bool
}

func (h *JustHasher) Reset() {
	h.content = []byte{}
	h.tree = false
}

func (h *JustHasher) Size() int {
	return Size
}

func (h *JustHasher) BlockSize() int {
	return BlockSize
}

func (h *JustHasher) Sum(b []byte) []byte {
	// b is always empty
	if h.tree {
		return append([]byte{0x74}, GitTreeID(h.content)...)
	}
	return append([]byte{0x62}, GitBlobID(h.content)...)
}

func (h *JustHasher) Write(p []byte) (int, error) {
	h.content = append(h.content, p...)
	return len(p), nil
}

func NewBlobHasher() hash.Hash {
	h := new(JustHasher)
	h.Reset()
	return h
}

func NewTreeHasher() hash.Hash {
	h := new(JustHasher)
	h.Reset()
	h.tree = true
	return h
}

func GitID(t string, content []byte) []byte {
	s := sha1.New()
	s.Write([]byte(fmt.Sprintf("%s %d", t, len(content))))
	s.Write([]byte{0})
	s.Write(content)
	return s.Sum([]byte{})
}

func GitBlobID(content []byte) []byte {
	return GitID("blob", content)
}

func GitTreeID(content []byte) []byte {
	return GitID("tree", content)
}
