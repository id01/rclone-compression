package press

import (
	"io"
	"os"
	"bufio"
	"testing"
//	"time"
)

const testFileName = "test/test.vdi"
const outFileName = "/volatile/other/Downloads/stuff.vdi"
const outFileName2 = "test/testSeek.vdi"

const Preset = "lz4"

func TestCompressFile(t *testing.T) {
	comp, err := NewCompressionPreset(Preset)
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
	comp, err := NewCompressionPreset(Preset)
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
	bufr := bufio.NewReaderSize(FileHandle, 12345678)
	for {
		_, err := io.CopyN(outFil, bufr, 123456)
//		b := make([]byte, 12345678)
//		_, err := FileHandle.Read(b)
//		outFil.Write(b)
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
	comp, err := NewCompressionPreset(Preset)
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
	comp, err := NewCompressionPreset(Preset)
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