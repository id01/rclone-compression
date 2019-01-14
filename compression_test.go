package press

import (
	"io"
	"os"
	"testing"
//	"time"
)

const testFileName = "test/test.vdi"
const outFileName = "test/testRead.vdi"
const outFileName2 = "test/testSeek.vdi"

// Gzip compression configuration
//const CompressionMode = GZIP_MIN // Compression mode
//const BlockSize = 131070 // We're using 4 bytes instead now! Here's a block size of 128KB-2B!

// XZ compression configuration
//const CompressionMode = XZ_IN_GZ // Compression mode
//const BlockSize = 1048576 // XZ needs larger block sizes to be more effective. Here's a 1MB block size.

// LZ4 compression configuration
const CompressionMode = LZ4_IN_GZ // Compression mode
const BlockSize = 262140 // 256KB-4B block size

func TestCompressFile(t *testing.T) {
	comp, err := NewCompression(CompressionMode, BlockSize)
	if err != nil {
		t.Fatal(err)
	}
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
	comp, err := NewCompression(CompressionMode, BlockSize)
	if err != nil {
		t.Fatal(err)
	}
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
//		_, err := io.CopyN(outFil, FileHandle, 123456)
		b := make([]byte, 12345678)
		_, err := FileHandle.Read(b)
		outFil.Write(b)
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
	comp, err := NewCompression(CompressionMode, BlockSize)
	if err != nil {
		t.Fatal(err)
	}
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
		FileHandle.Seek(12345678, io.SeekCurrent) // 93323248
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
	comp, err := NewCompression(CompressionMode, BlockSize)
	if err != nil {
		t.Fatal(err)
	}
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