package press

import (
	"io"
	"os"
	"testing"
//	"time"
)

const testFileName = "test.txt"
const outFileName = "testRead.txt"
const outFileName2 = "testSeek.txt"

func TestCompressFile(t *testing.T) {
	inFile, err := os.Open(testFileName)
	if err != nil {
		t.Fatal(err)
	}
	outFil, err := os.Create(testFileName+GetFileExtension())
	if err != nil {
		t.Fatal(err)
	}
	CompressFile(inFile, 0, outFil)
	outFil.Close()
}

func TestDecompressFile(t *testing.T) {
	inFileInfo, err := os.Stat(testFileName+GetFileExtension())
	if err != nil {
		t.Fatal(err)
	}
	inFile, err := os.Open(testFileName+GetFileExtension())
	if err != nil {
		t.Fatal(err)
	}
	outFil, err := os.Create(outFileName)
	if err != nil {
		t.Fatal(err)
	}
	FileHandle, decompressedSize, err := DecompressFile(inFile, inFileInfo.Size())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Decompressed size: %d\n", decompressedSize)
	for {
		_, err := io.CopyN(outFil, FileHandle, 123456)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	inFile.Close()
	outFil.Close()
}

func TestSeek(t *testing.T) {
	inFileInfo, err := os.Stat(testFileName+GetFileExtension())
	if err != nil {
		t.Fatal(err)
	}
	inFile, err := os.Open(testFileName+GetFileExtension())
	if err != nil {
		t.Fatal(err)
	}
	outFil, err := os.Create(outFileName2)
	if err != nil {
		t.Fatal(err)
	}
	FileHandle, decompressedSize, err := DecompressFile(inFile, inFileInfo.Size())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Decompressed size: %d\n", decompressedSize)
//	time.Sleep(time.Second*5)
	for {
		FileHandle.Seek(1048560, io.SeekCurrent)
		_, err := io.CopyN(outFil, FileHandle, 16)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	inFile.Close()
	outFil.Close()
}

func TestFileCompressionInfo(t *testing.T) {
	inFile, err := os.Open(testFileName)
	if err != nil {
		t.Fatal(err)
	}
	inFile2, err := os.Open(testFileName+GetFileExtension())
	if err != nil {
		t.Fatal(err)
	}
	_, extension, err := GetFileCompressionInfo(inFile)
	t.Logf("Extension for uncompressed: %s\n", extension)
	_, extension, err = GetFileCompressionInfo(inFile2)
	t.Logf("Extension for compressed: %s\n", extension)
	inFile.Close()
}