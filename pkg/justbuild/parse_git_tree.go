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

func ToDirectoryMessage(entries []byte) (*remoteexecution.Directory, error) {
	var directory remoteexecution.Directory

	dir_tag := []byte(fmt.Sprintf("%o", 0o40000))
	exe_file := []byte(fmt.Sprintf("%o", 0o100755))
	reg_file := []byte(fmt.Sprintf("%o", 0o100644))
	const (
		len_dir_tag  = 5
		len_file_tag = 6
		len_hash     = 20
	)
	for pos := 0; pos < len(entries); {
		if bytes.Contains(entries[pos:pos+len_dir_tag], dir_tag) {
			//is a dir
			pos += len_dir_tag + 1
			name, x := getName(entries[pos:])
			pos += x
			hash := hex.EncodeToString(entries[pos : pos+len_hash])
			pos += len_hash
			directory.Directories = append(directory.Directories,
				&remoteexecution.DirectoryNode{
					Name:   name,
					Digest: &remoteexecution.Digest{Hash: TreeMarker + hash},
				})
		} else if bytes.Contains(entries[pos:pos+len_file_tag], exe_file) {
			// is an executable file
			pos += len_file_tag + 1
			name, x := getName(entries[pos:])
			pos += x
			hash := hex.EncodeToString(entries[pos : pos+20])
			pos += len_hash
			directory.Files = append(directory.Files, &remoteexecution.FileNode{
				Name:         name,
				Digest:       &remoteexecution.Digest{Hash: BlobMarker + hash},
				IsExecutable: true,
			})
		} else if bytes.Contains(entries[pos:pos+len_file_tag], reg_file) {
			pos += len_file_tag + 1
			name, x := getName(entries[pos:])
			pos += x
			hash := hex.EncodeToString(entries[pos : pos+20])
			pos += len_hash
			directory.Files = append(directory.Files, &remoteexecution.FileNode{
				Name:         name,
				Digest:       &remoteexecution.Digest{Hash: BlobMarker + hash},
				IsExecutable: false,
			})

		} else {
			return nil, fmt.Errorf("unknown tag %s", string(entries[pos:pos+len_file_tag]))
		}
	}
	return &directory, nil
}
