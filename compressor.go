// Note: I am not a go programmer; this may need some refining.
// It's my first time writing golang :)
package press // POC Compressor

import (
	"log"
	"io"
	"errors"
	"hash/crc32"
	"math"
	"bytes"
	"encoding/binary"

	"github.com/rasky/multigz"
)

// Constants
//const CompressionLevel = 0 // Worst-case compression
const CompressionLevel = -1 // Default compression
//const CompressionLevel = 9 // Max compression
const BlockSize = 65502 // Let's only use a single size (2 bytes) for now

// Converts uint16 to bytes (little endian)
func uint16ToBytes(n uint16) []byte {
	return []byte{byte(n%256), byte(n>>8)}
}

// Converts bytes to uint16 (little endian)
func bytesToUint16(n []byte) uint16 {
	return uint16(n[0])+(uint16(n[1])<<8)
}

// Converts bytes to uint32 (little endian)
func bytesToUint32(n []byte) uint32 {
	res := uint32(0)
	for i := 3; i>=0; i-- {
		res <<= 8
		res += uint32(n[i])
	}
	return res
}

// Our format will be [gzip data] [block data]
// Each block is stored as [checksum (4 bytes)+size (2 bytes)] in block data
// Block data is structured in the form [0xabcd] [block] [block] ... [block] [sizeOfBlockData (4 bytes)] [fake gzip footer (8 bytes)]
// Compresses a file. Argument "size" is ignored.
func CompressFile(in io.Reader, size int64, out io.Writer) error {
	// Initialize writer to tempfile
	var outw multigz.Writer
	outw, err := multigz.NewWriterLevel(out, CompressionLevel, BlockSize) // I suppose we'd to well to line up BlockSize with the compression block size;
									    // this will make Offset always equal BlockSize for any blocks
	if err != nil {
		return err
	}

	// Write gzip
	var blockStartPrev int64 = int64(0)
	var blockData []byte = make([]byte, 2) // Prepend a garbage magic number to make sure this isn't recognized as a part of the gzip file (and so it's easier for me to find)
	blockData[0] = 0xab
	blockData[1] = 0xcd
	// Initialize file hash (for gzip footer after block data)
	fileHash := crc32.NewIEEE()
	var lastBlockDecompressedSize uint32
	for {
		// Initialize hash, compress block and add to hash (and file hash)
		hash := crc32.NewIEEE()
		n, err := io.CopyN(io.MultiWriter(outw, hash, fileHash), in, BlockSize)
		checksum := hash.Sum([]byte{})

		// Get offset and process
		offset := outw.Offset()
		if offset.Off != BlockSize && err != io.EOF {
			log.Println("error1")
			return errors.New("Offset is expected equal BlockSize and this isn't the last block. This error should never occur.")
		}
		blockStart := offset.Block
		blockSizeLarge := blockStart - blockStartPrev
		log.Printf("%d %d\n", n, blockSizeLarge)
		if blockSizeLarge > math.MaxUint16 {
			log.Println("error2")
			return errors.New("Compressed block size is larger than MaxUint16. This error should never occur.")
		}
		blockSize := uint16(blockSizeLarge) // Note: This is actually the uncompressed length of the PREVIOUS block.
						    // If this is the first block, this value is always 0

		// Append block size and checksum to block data and set current block start as previous block start
		blockData = append(blockData, append(uint16ToBytes(blockSize), checksum...)...)
		blockStartPrev = blockStart

		// If this is the last block, copy over the uncompressed size of the block (we need this for both the gzip
		// footer and our own processing)
		if err == io.EOF {
			log.Printf("%d %d %d\n", n, byte(n%256), byte(n/256))
			lastBlockDecompressedSize = uint32(n)
			break
		}
	}

	// Close gzip Writer, and append block data, length of block data (4 bytes). This will be recognized by gunzip as "trailing garbage"
	outw.Close()
	out.Write(blockData)
	blockDataLen := uint32(len(blockData))
	binary.Write(out, binary.LittleEndian, blockDataLen)

	// Write another (fake) footer to increase compatibility
	out.Write(fileHash.Sum([]byte{})) // CRC-32 checksum of file
	binary.Write(out, binary.LittleEndian, lastBlockDecompressedSize) // Decompressed size of last block

	// Return success
	return nil
}

// ReadSeeker implementation for decompression
type Decompressor struct {
	cursorPos *int64		// The current location we have seeked to
	blockStarts []int64		// The start of each block. These will be recovered from the block sizes
	blockChecksums []uint32		// Checksums should be uint32s
	numBlocks uint32		// Number of blocks
	decompressedSize int64		// Decompressed size of the file.
	in *multigz.Reader		// Input
}

const InitialChunkLength = 2048 // Length of initial chunk to get. This will support up to ~20MB indexing without additional fetching of data
const TrailingBytes = 4+8 // Number of bytes after block data (size index + fake footer) that aren't part of block data
			  // Note: We assume size index will always be the first value and fake footer will always be the last in this

// Initializes decompressor. Takes 2 reads if length of block data < InitialChunkLength or 3 reads otherwise
func (d* Decompressor) init(in io.ReadSeeker, size int64) error {
	// Initialize cursor position
	d.cursorPos = new(int64)

	// Read last chunk of file
	lastChunkStart := size-InitialChunkLength
	if lastChunkStart < 0 {
		lastChunkStart = 0
	}
	in.Seek(lastChunkStart, io.SeekStart)
	lastChunk := make([]byte, InitialChunkLength)
	n, err := in.Read(lastChunk)
	if err != nil {
		log.Printf("n = %d\n", n)
		return err
	}
	if n != InitialChunkLength {
		log.Printf("n = %d\n", n)
		lastChunk = lastChunk[:n]
	}
	lastChunkLen := uint32(len(lastChunk))

	// Get metadata size
	blockDataLen := bytesToUint32(lastChunk[lastChunkLen-TrailingBytes:lastChunkLen-TrailingBytes+4])

	// Isolate the block data
	var blockData []byte
	log.Printf("lastChunkLen, blockDataLen = %d, %d", lastChunkLen, blockDataLen)
	if blockDataLen < InitialChunkLength { // If we already have all the data, use it
		blockData = lastChunk[lastChunkLen-TrailingBytes-blockDataLen:lastChunkLen-TrailingBytes]
	} else { // If we don't, get it
		blockData = make([]byte, blockDataLen)
		in.Seek(-TrailingBytes-int64(blockDataLen), io.SeekEnd)
		in.Read(blockData)
	}

	// Parse the block data
	log.Printf("metadata len = %d\n", bytesToUint32(lastChunk[lastChunkLen-4:]))
	if blockData[0] != 0xab || blockData[1] != 0xcd {
		return errors.New("Unrecognized magic number; file may be corrupted")
	}
	if blockData[2] != 0x00 || blockData[3] != 0x00 {
		return errors.New("Length before first block should always be 0; file may be corrupted")
	}
	if (blockDataLen-2)%6 != 0 {
		return errors.New("Length of block data should be 2 plus a multiple of 6; file may be corrupted")
	}
	d.numBlocks = (blockDataLen-2)/6
	log.Printf("numblocks = %d\n", d.numBlocks)
	d.blockStarts = make([]int64, d.numBlocks)
	d.blockChecksums = make([]uint32, d.numBlocks)
	currentBlockPosition := int64(0)
	for i := uint32(0); i < d.numBlocks; i++ { // Loop through block data, getting checksums and starts of blocks.
		bs := i*6+4 // Location of start of block data. Note: We're skipping the length before the first block so we don't record two 0's
			    // in currentBlockPosition
		currentBlockChecksum := bytesToUint32(blockData[bs:bs+4])
		currentBlockSize := bytesToUint16(blockData[bs+4:bs+6])
		d.blockStarts[i] = currentBlockPosition // Note: Remember that the first entry will always be 0
		d.blockChecksums[i] = currentBlockChecksum
		currentBlockPosition += int64(currentBlockSize) // Note: We increase the current block position after
							// recording the size (the size is for the current block this time, though)
	}

	log.Printf("Block Starts: %v\n", d.blockStarts)
	log.Printf("Block Checksums: %v\n", d.blockChecksums)

	// Get uncompressed size of last block and derive uncompressed size of file
	lastBlockRawSize := bytesToUint32(lastChunk[lastChunkLen-4:])
	d.decompressedSize = int64((d.numBlocks-1) * BlockSize + uint32(lastBlockRawSize))

	// Initialize cursor position and create reader
	*d.cursorPos = 0
	in.Seek(0, io.SeekStart)
	din, err := multigz.NewReader(in)
	d.in = din

	return err
}

// Reads data using a decompressor
func (d Decompressor) Read(p []byte) (int, error) {
	log.Printf("Cursor pos before: %d\n", *d.cursorPos)
	// Check if we're at the end of the file or before the beginning of the file
	if *d.cursorPos >= d.decompressedSize || *d.cursorPos < 0 {
		log.Println("Out of bounds EOF")
		return 0, io.EOF
	}

	// Get block range to read and seek to it in multigz
	var mgzOffset multigz.Offset
	blockToRead := *d.cursorPos / BlockSize
	blockOffset := *d.cursorPos % BlockSize
	if blockToRead > 0 {
		mgzOffset.Block = d.blockStarts[*d.cursorPos / BlockSize - 1]
		mgzOffset.Off = BlockSize
	} else {
		mgzOffset.Block = 0
		mgzOffset.Off = 0
	}
	err := d.in.Seek(mgzOffset)
	if err != nil {
		return 0, err
	}

	// Get number of blocks to read
	bytesToRead := len(p)
	blocksToRead := (int64(bytesToRead) + blockOffset) / int64(BlockSize) + 1

	log.Printf("bytesTR = %d, blocksTR = %d, block#TR = %d, blockOff = %d\n", bytesToRead, blocksToRead, blockToRead, blockOffset)

	// Read block range
	// Our reader is currently at d.blockStarts[*d.cursorPos / BlockSize - 1] + BlockSize or 0.
	// We want to read to the beginning of d.blockStarts[*d.cursorPos / BlockSize + blocksToRead] or to the end
	currBlock := *d.cursorPos / BlockSize
	var blockBytes []byte;

	// Read blocksToRead blocks and slice off unused bytes if there are any
	blockBytes = make([]byte, blocksToRead * BlockSize)
	n, _ := d.in.Read(blockBytes)
	if n != int(blocksToRead * BlockSize) {
		blockBytes = blockBytes[:n]
	}
	// Increment cursor position based on whichever is smaller: bytes read or bytes to read
	bytesRead := n-int(blockOffset)
	if bytesRead < bytesToRead {
		*d.cursorPos += int64(bytesRead)
	} else {
		*d.cursorPos += int64(bytesToRead)
	}
	log.Printf("bytesRead = %d, bytesToRead = %d\n", bytesRead, bytesToRead)

//	log.Printf("%d %s\n", len(blockBytes), string(blockBytes))

	// Integrity verification of blocks
	blockReader := bytes.NewReader(blockBytes)
	for i := int64(0); i < blocksToRead; i++ {
		hash := crc32.NewIEEE()
		n, err := io.CopyN(hash, blockReader, BlockSize)
		sum := hash.Sum([]byte{})
		log.Printf("Bytes checksummed = %d\n", n)
		log.Printf("Checksum %d vs %d\n", bytesToUint32(sum), d.blockChecksums[currBlock + i])
		if bytesToUint32(sum) != d.blockChecksums[currBlock + i] { // Checksum failed
			return 0, errors.New("Checksum verification failed")
		}
		if err == io.EOF {
			break
		}
	}

	// Copy blockBytes over to output
	var copiedBytes int
	for copiedBytes = 0; (copiedBytes < bytesToRead) && (copiedBytes+int(blockOffset) < len(blockBytes)); copiedBytes++ {
		p[copiedBytes] = blockBytes[copiedBytes+int(blockOffset)]
	}

	// Check if we've reached the end of the file and return accordingly
	log.Printf("Cursor pos = %d\n", *d.cursorPos)
	if *d.cursorPos >= d.decompressedSize {
		log.Println("EOF")
		return copiedBytes, io.EOF
	}
	return copiedBytes, nil
}

// Seeks to a location in compressed stream
func (d Decompressor) Seek(offset int64, whence int) (int64, error) {
	// Seek to offset in cursorPos
	if whence == io.SeekStart {
		*d.cursorPos = offset
	} else if whence == io.SeekCurrent {
		*d.cursorPos += offset
	} else if whence == io.SeekEnd {
		*d.cursorPos = d.decompressedSize + offset
	}
	// Return
	return offset, nil
}

// Decompresses a file. Argument "size" is very useful here.
func DecompressFile(in io.ReadSeeker, size int64) (FileHandle io.ReadSeeker, decompressedSize int64, err error) {
	var decompressor Decompressor
	err = decompressor.init(in, size)
	return decompressor, decompressor.decompressedSize, err
}