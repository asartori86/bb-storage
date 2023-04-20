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
const MarkerSizeBytes = 1

// placeholder for the justbuild bare function. justbuild actually uses 2 hash
// funcitons: one for trees and one for blobs

// when this bare function is detected, depending on the context, the user
// should call the blob or tree function
type JustBareHasher struct{}

func (x *JustBareHasher) Reset() {
	panic("JustBareHasher.Reset has been invoked. This is just a placeholder, the user should, depending on the context use JustBlobHasher or JustTreeHasher")
}

func (x *JustBareHasher) Size() int {
	return Size
}

func (x *JustBareHasher) BlockSize() int {
	return BlockSize
}

func (x *JustBareHasher) Sum(b []byte) []byte {
	panic("JustBareHasher.Sum has been invoked. This is just a placeholder, the user should, depending on the context use JustBlobHasher or JustTreeHasher")
	return nil
}

func (x *JustBareHasher) Write(p []byte) (int, error) {
	panic("JustBareHasher.Write has been invoked. This is just a placeholder, the user should, depending on the context use JustBlobHasher or JustTreeHasher")
	return 0, nil
}

func NewJustBareHasher() hash.Hash {
	h := new(JustBareHasher)
	return h
}

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
