package justbuild

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

func getName(data []byte) (string, int) {
	pos := bytes.Index(data, []byte("\x00"))
	if pos >= 0 {
		return string(data[:pos]), pos + 1
	}
	return "", pos
}

type BlobType int

const (
	RegularFile    BlobType = 0
	ExecutableFile BlobType = 1
	Tree           BlobType = 2
	Symlink        BlobType = 3
)

func IsJustbuildTree(hash string) bool {
	return len(hash) == Size*2 && hash[:2] == TreeMarker
}

func GetTaggedHashes(entries []byte) ([]string, []BlobType, []string, error) {
	if len(entries) == 0 {
		return nil, nil, nil, fmt.Errorf("got empty tree content. This likely means that the bytesize of the corresponding blob has been deduced to be zero, while it is not the case")
	}
	var hashes []string
	var types []BlobType
	var names []string
	dirTag := []byte(fmt.Sprintf("%o", 0o40000))
	exeFile := []byte(fmt.Sprintf("%o", 0o100755))
	regFile := []byte(fmt.Sprintf("%o", 0o100644))
	symlinkTag := []byte(fmt.Sprintf("%o", 0o120000))
	const (
		lenDirTag  = 5
		lenFileTag = 6
		lenSymTag  = 6
		lenHash    = 20
	)
	for pos := 0; pos < len(entries); {
		if bytes.Contains(entries[pos:pos+lenDirTag], dirTag) {
			//is a dir
			pos += lenDirTag + 1
			name, x := getName(entries[pos:])
			pos += x
			hash := hex.EncodeToString(entries[pos : pos+lenHash])
			hashes = append(hashes, TreeMarker+hash)
			types = append(types, Tree)
			names = append(names, name)
			pos += lenHash
		} else if bytes.Contains(entries[pos:pos+lenFileTag], exeFile) {
			// is an executable file
			pos += lenFileTag + 1
			name, x := getName(entries[pos:])
			pos += x
			hash := hex.EncodeToString(entries[pos : pos+lenHash])
			hashes = append(hashes, BlobMarker+hash)
			types = append(types, ExecutableFile)
			names = append(names, name)
			pos += lenHash
		} else if bytes.Contains(entries[pos:pos+lenFileTag], regFile) {
			pos += lenFileTag + 1
			name, x := getName(entries[pos:])
			pos += x
			hash := hex.EncodeToString(entries[pos : pos+lenHash])
			hashes = append(hashes, BlobMarker+hash)
			types = append(types, RegularFile)
			names = append(names, name)
			pos += lenHash
		} else if bytes.Contains(entries[pos:pos+lenSymTag], symlinkTag) {
			pos += lenSymTag + 1
			name, x := getName(entries[pos:])
			pos += x
			hash := hex.EncodeToString(entries[pos : pos+lenHash])
			hashes = append(hashes, BlobMarker+hash)
			types = append(types, Symlink)
			names = append(names, name)
			pos += lenHash
		} else {
			return nil, nil, nil, fmt.Errorf("unknown tag %s", string(entries[pos:pos+lenFileTag]))
		}
	}
	return hashes, types, names, nil
}
