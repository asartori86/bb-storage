package justbuild

import (
	"bytes"
	"encoding/hex"
	"fmt"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
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
)

func IsJustbuildTree(hash string) bool {
	return len(hash) == Size*2 && hash[:2] == TreeMarker
}

func GetAllTaggedHashes(entries []byte) ([]string, []BlobType, []string, error) {
	if len(entries) == 0 {
		return nil, nil, nil, fmt.Errorf("got empty tree content. This likely means that the bytesize of the corresponding blob has been deduced to be zero, while it is not the case")
	}
	var hashes []string
	var types []BlobType
	var names []string
	dirTag := []byte(fmt.Sprintf("%o", 0o40000))
	exeFile := []byte(fmt.Sprintf("%o", 0o100755))
	regFile := []byte(fmt.Sprintf("%o", 0o100644))
	const (
		lenDirTag  = 5
		lenFileTag = 6
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
		} else {
			return nil, nil, nil, fmt.Errorf("unknown tag %s", string(entries[pos:pos+lenFileTag]))
		}
	}
	return hashes, types, names, nil
}

func ToDirectoryMessage(entries []byte) (*remoteexecution.Directory, error) {
	var directory remoteexecution.Directory
	if len(entries) == 0 {
		return &directory, nil
	}
	hashes, types, names, err := GetAllTaggedHashes(entries)
	if err != nil {
		return nil, err
	}
	for i, hash := range hashes {
		blobType := types[i]
		name := names[i]
		if blobType == Tree {
			directory.Directories = append(directory.Directories,
				&remoteexecution.DirectoryNode{
					Name:   name,
					Digest: &remoteexecution.Digest{Hash: hash},
				})
		} else if blobType == ExecutableFile {
			directory.Files = append(directory.Files, &remoteexecution.FileNode{
				Name:         name,
				Digest:       &remoteexecution.Digest{Hash: hash},
				IsExecutable: true,
			})
		} else if blobType == RegularFile {
			directory.Files = append(directory.Files, &remoteexecution.FileNode{
				Name:         name,
				Digest:       &remoteexecution.Digest{Hash: hash},
				IsExecutable: false,
			})

		} else {
			return nil, fmt.Errorf("unknown tag %v", blobType)
		}
	}
	return &directory, nil
}
