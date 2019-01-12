package press

import (
	"io"
	"os"
	"testing"
//	"time"
)

const testFileName = "test/test.txt"
const outFileName = "test/testRead.txt"
const outFileName2 = "test/testSeek.txt"

// Gzip compression configuration
const CompressionMode = GZIP_DEFAULT // Compression mode
const BlockSize = 131072 // We're using 4 bytes instead now! Here's a block size of 128KB!

// XZ compression configuration
//const CompressionMode = XZ_IN_GZ // Compression mode
//const BlockSize = 1048576 // XZ needs larger block sizes to be more effective. Here's a 1MB block size.

// LZ4 compression configuration
//const CompressionMode = LZ4_IN_GZ // Compression mode
//const BlockSize = 131072 // Same as gzip.

func TestCompressFile(t *testing.T) {
	comp := NewCompression(CompressionMode, BlockSize)
	inFile, err := os.Open(testFileName)
	if err != nil {
		t.Fatal(err)
	}
	outFil, err := os.Create(testFileName+comp.GetFileExtension())
	if err != nil {
		t.Fatal(err)
	}
	comp.CompressFile(inFile, 0, outFil)
	outFil.Close()
}

func TestDecompressFile(t *testing.T) {
	comp := NewCompression(CompressionMode, BlockSize)
	inFileInfo, err := os.Stat(testFileName+comp.GetFileExtension())
	if err != nil {
		t.Fatal(err)
	}
	inFile, err := os.Open(testFileName+comp.GetFileExtension())
	if err != nil {
		t.Fatal(err)
	}
	outFil, err := os.Create(outFileName)
	if err != nil {
		t.Fatal(err)
	}
	FileHandle, decompressedSize, err := comp.DecompressFile(inFile, inFileInfo.Size())
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
	comp := NewCompression(CompressionMode, BlockSize)
	inFileInfo, err := os.Stat(testFileName+comp.GetFileExtension())
	if err != nil {
		t.Fatal(err)
	}
	inFile, err := os.Open(testFileName+comp.GetFileExtension())
	if err != nil {
		t.Fatal(err)
	}
	outFil, err := os.Create(outFileName2)
	if err != nil {
		t.Fatal(err)
	}
	FileHandle, decompressedSize, err := comp.DecompressFile(inFile, inFileInfo.Size())
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
	comp := NewCompression(CompressionMode, BlockSize)
	inFile, err := os.Open(testFileName)
	if err != nil {
		t.Fatal(err)
	}
	inFile2, err := os.Open(testFileName+comp.GetFileExtension())
	if err != nil {
		t.Fatal(err)
	}
	_, extension, err := comp.GetFileCompressionInfo(inFile)
	t.Logf("Extension for uncompressed: %s\n", extension)
	_, extension, err = comp.GetFileCompressionInfo(inFile2)
	t.Logf("Extension for compressed: %s\n", extension)
	inFile.Close()
}