package justbuild_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/justbuild"
)

func TestBlobHash(t *testing.T) {
	s := "foo"
	refStr := "19102815663d23f8b75a47e7a01965dcdc96468c"
	ref, _ := hex.DecodeString(refStr)
	if x := justbuild.GitBlobID([]byte(s)); !bytes.Equal(x, ref) {
		t.Errorf("Expected %s but got %s\n", refStr, hex.EncodeToString(x))
	}
}

func TestTreeHash(t *testing.T) {
	barTxtStr := "5716ca5987cbf97d6bb54920bea6adde242d87e6"
	barTxt, _ := hex.DecodeString(barTxtStr)
	s := fmt.Sprintf("%o %s\x00", 0o100644, "bar.txt")
	bar := append([]byte(s), barTxt...)
	barRefStr := "8535775197eeced6f90e9116618c61472ebccb9f"
	barRef, _ := hex.DecodeString(barRefStr)
	if x := justbuild.GitTreeID(bar); !bytes.Equal(x, barRef) {
		t.Errorf("Expected %s but got %s\n", barRefStr, hex.EncodeToString(x))
	}
}

func TestTreeHash2(t *testing.T) {
	barTxtStr := "5716ca5987cbf97d6bb54920bea6adde242d87e6"
	barTxt, _ := hex.DecodeString(barTxtStr)
	s := fmt.Sprintf("%o %s\x00", 0o100644, "bar.txt")
	bar := append([]byte(s), barTxt...)
	barRefStr := "8535775197eeced6f90e9116618c61472ebccb9f"
	barRef, _ := hex.DecodeString(barRefStr)
	if x := justbuild.GitTreeID(bar); !bytes.Equal(x, barRef) {
		t.Errorf("Expected %s but got %s\n", barRefStr, hex.EncodeToString(x))
	}
}

func DeSerialize(entries []byte) string {
	s := "\n"
	dir_tag := []byte(fmt.Sprintf("%o", 0o40000))
	exe_file := []byte(fmt.Sprintf("%o", 0o100755))
	reg_file := []byte(fmt.Sprintf("%o", 0o100644))
	fmt.Println(len(dir_tag), len(exe_file), len(reg_file))
	for pos := 0; pos < len(entries); {
		if bytes.Contains(entries[pos:pos+6], dir_tag) {
			//is a dir
			pos += 6
			s += "tree: " + string(entries[pos:pos+3]) + "\t"
			pos += 4
			hash := hex.EncodeToString(entries[pos : pos+20])
			s += hash + "\n"
			pos += 20
		} else {
			// is a file
			pos += 7
			s += "blob: " + string(entries[pos:pos+7]) + "\t"
			pos += 8
			hash := hex.EncodeToString(entries[pos : pos+20])
			s += hash + "\n"
			pos += 20

		}
	}
	return s
}

func TestTreeHash3(t *testing.T) {
	fooTxtStr := "257cc5642cb1a054f08cc83f2d943e56fd3ebe99"
	fooTxt, _ := hex.DecodeString(fooTxtStr)

	barStr := "8535775197eeced6f90e9116618c61472ebccb9f"
	bar, _ := hex.DecodeString(barStr)

	entries := append([]byte(fmt.Sprintf("%o %s\x00", 0o40000, "bar")), bar...)
	entries = append(entries, append([]byte(fmt.Sprintf("%o %s\x00", 0o100644, "foo.txt")), fooTxt...)...)

	refStr := "a7f6c6416b73d97b06aade7851a4a989761959f6"
	ref, _ := hex.DecodeString(refStr)
	if x := justbuild.GitTreeID(entries); !bytes.Equal(x, ref) {
		t.Errorf("Expected %s but got %s\n", refStr, hex.EncodeToString(x))
	}
}

func TestBuildTree(t *testing.T) {
	dir := "data"
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Errorf("Could not open directory %s\n", dir)
		t.Error(err)
	}
	for _, f := range files {
		t.Log(f)
	}
}
