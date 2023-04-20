package emptyblobs

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
	// md5 empty srting
	EmptyBlobs["d41d8cd98f00b204e9800998ecf8427e"] = true
}

func IsEmptyBlob(hash string) bool {
	return EmptyBlobs[hash]
}
