// Note: I am not a go programmer; this may need some refining.
// It's my first time writing golang :)
package press // POC Compressor

import (
	"log"
	"io"
	"io/ioutil"
	"errors"
	"bytes"
	"compress/gzip"

	"github.com/rasky/multigz"
)

// Constants
//const CompressionLevel = 0 // Worst-case compression
const CompressionLevel = -1 // Default compression
//const CompressionLevel = 9 // Max compression
const BlockSize = 131072 // We're using 4 bytes instead now! Here's a block size of 128KB!

// Converts uint16 to bytes (little endian)
func uint16ToBytes(n uint16) []byte {
	return []byte{byte(n%256), byte(n>>8)}
}

// Converts bytes to uint16 (little endian)
func bytesToUint16(n []byte) uint16 {
	return uint16(n[0])+(uint16(n[1])<<8)
}

// Converts uint32 to bytes (little endian)
func uint32ToBytes(n uint32) []byte {
	return append(uint16ToBytes(uint16(n%65536)), uint16ToBytes(uint16(n>>16))...)
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

// Size of gzip header and footer for gzip files that are storing block data in extra data fields
const GzipHeaderSize = 10
const GzipDataAndFooterSize = 10
// Splits data into extra data in empty gzip files, followed by a gzip file storing the total length of all the prior gzip files as a uint32
func gzipExtraify(in io.Reader, out io.Writer) {
// These should be constant
var gzipHeaderData = []byte{0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03} // A gzip header that allows for extra data
var gzipContentAndFooter = []byte{0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00} // Empty gzip content and footer

	// Loop through the data, splitting it into up to 65535-byte chunks, then adding it to an empty gzip file as extra data
	totalLength := uint32(0)
	for {
		currGzipData := make([]byte, 65535)
		n, err := in.Read(currGzipData) // n is the length of the extra data that will be added
		if err == io.EOF {
			break
		}
		currGzipData = append(append(gzipHeaderData, uint16ToBytes(uint16(n))...), // n bytes
			append(currGzipData[:n], gzipContentAndFooter...)...) // Data and footer
		totalLength += uint32(len(currGzipData))
		out.Write(currGzipData)
	}
	out.Write(append(gzipHeaderData, []byte{0x04, 0x00}...)) // 4 bytes
	out.Write(append(uint32ToBytes(totalLength), gzipContentAndFooter...))
}

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
	var blockData []byte = make([]byte, 0)
	for {
		// Compress block
		n, err := io.CopyN(outw, in, BlockSize)

		// Get offset and process
		offset := outw.Offset()
		log.Printf("Block = %d, Off = %d\n", offset.Block, offset.Off)
		if err != io.EOF && offset.Off != 0 {
			log.Println("error1")
			return errors.New("Offset is expected to equal 0 if this isn't the last block. This error should never occur.")
		}
		blockStart := offset.Block
		blockSize := uint32(blockStart - blockStartPrev) // Note: This is actually the uncompressed length of the PREVIOUS block.
								 // If this is the first block, this value is always 0
		log.Printf("%d %d\n", n, blockSize)

		// Append block size to block data and move current block start to previous block start
		blockData = append(blockData, uint32ToBytes(blockSize)...)
		blockStartPrev = blockStart

		// If this is the last block, add the size of the last block to the end of blockData and break
		if err == io.EOF {
			log.Printf("%d %d %d\n", n, byte(n%256), byte(n/256))
			blockData = append(blockData, uint32ToBytes(uint32(n))...)
			break
		}
	}

	// Close gzip Writer for data
	outw.Close()

	// Create gzip file containing block index data, stored in buffer
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write(blockData); err != nil {
		panic(err)
	}
	if err := gz.Flush(); err != nil {
		panic(err)
	}
	if err := gz.Close(); err != nil {
		panic(err)
	}

	// Append extra data gzips to end of out
	gzipExtraify(bytes.NewReader(b.Bytes()), out)

	// Return success
	return nil
}

// ReadSeeker implementation for decompression
type Decompressor struct {
	cursorPos *int64		// The current location we have seeked to
	blockStarts []int64		// The start of each block. These will be recovered from the block sizes
	numBlocks uint32		// Number of blocks
	decompressedSize int64		// Decompressed size of the file.
	in *multigz.Reader		// Input
}

// Decompression constants
const LengthOffsetFromEnd = GzipDataAndFooterSize+4 // How far the 4-byte length of gzipped data is from the end
const TrailingBytes = LengthOffsetFromEnd+2+GzipHeaderSize // This is the total size of the last gzip file in the stream, which is not included in the length of gzipped data

// Initializes decompressor. Takes 2 reads if length of block data < InitialChunkLength or 3 reads otherwise
func (d* Decompressor) init(in io.ReadSeeker, size int64) error {
	// Initialize cursor position
	d.cursorPos = new(int64)

	// Read length of gzipped block data in gzip extra data fields
	in.Seek(size-LengthOffsetFromEnd, io.SeekStart)
	gzippedBlockDataLenBytes := make([]byte, 4)
	_, err := in.Read(gzippedBlockDataLenBytes)
	if err != nil {
		return err
	}
	gzippedBlockDataLen := bytesToUint32(gzippedBlockDataLenBytes)

	// Get gzipped block data in gzip extra data fields
	log.Printf("size = %d, gzippedBlockDataLen = %d\n", size, gzippedBlockDataLen)
	in.Seek(size-TrailingBytes-int64(gzippedBlockDataLen), io.SeekStart)
	gzippedBlockData := make([]byte, gzippedBlockDataLen)
	in.Read(gzippedBlockData)

	// Get raw gzipped block data
	gzippedBlockDataRaw := make([]byte, 0)
	gzipHeaderDummy := make([]byte, GzipHeaderSize)
	gzipExtraDataLenBytes := make([]byte, 2)
	gzipDataAndFooterDummy := make([]byte, GzipDataAndFooterSize)
	gzippedBlockDataRawReader := bytes.NewReader(gzippedBlockData)
	for {
		// This read and possibly the last read are the only ones which can EOF
		_, err := gzippedBlockDataRawReader.Read(gzipHeaderDummy)
		if err == io.EOF {
			break
		}
		// Note: These reads should never EOF
		gzippedBlockDataRawReader.Read(gzipExtraDataLenBytes)
		gzipExtraDataLen := bytesToUint16(gzipExtraDataLenBytes)
		log.Printf("%d", gzipExtraDataLen)
		gzipExtraData := make([]byte, gzipExtraDataLen)
		gzippedBlockDataRawReader.Read(gzipExtraData)
		gzippedBlockDataRaw = append(gzippedBlockDataRaw, gzipExtraData...)
		// Read the footer. This may EOF
		_, err = gzippedBlockDataRawReader.Read(gzipDataAndFooterDummy)
		if err == io.EOF {
			break
		}
	}

	// Decompress gzipped block data
	blockDataReader, err := gzip.NewReader(bytes.NewReader(gzippedBlockDataRaw))
	if err != nil {
		return err
	}
	blockData, err := ioutil.ReadAll(blockDataReader)
	if err != nil {
		return err
	}

	// Parse the block data
	blockDataLen := len(blockData)
	log.Printf("metadata len = %d\n", blockDataLen)
	if blockDataLen%4 != 0 {
		return errors.New("Length of block data should be a multiple of 4; file may be corrupted")
	}
	d.numBlocks = uint32((blockDataLen-4)/4)
	log.Printf("numblocks = %d\n", d.numBlocks)
	d.blockStarts = make([]int64, d.numBlocks)
	currentBlockPosition := int64(0)
	for i := uint32(0); i < d.numBlocks; i++ { // Loop through block data, getting starts of blocks.
		bs := i*4 // Location of start of data for our current block
		d.blockStarts[i] = currentBlockPosition // Note: Remember that the first entry can be anything now, but we're making the first
							// of this array still always 0 for easier indexing
		currentBlockSize := bytesToUint32(blockData[bs:bs+4])
		currentBlockPosition += int64(currentBlockSize) // Note: We increase the current block position after
							// recording the size (the size is for the current block this time, though)
	}

	log.Printf("Block Starts: %v\n", d.blockStarts)

	// Get uncompressed size of last block and derive uncompressed size of file
	lastBlockRawSize := bytesToUint32(blockData[blockDataLen-4:])
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
	// Check if this is off the ends of the file
	blockNumber := *d.cursorPos / BlockSize
	if blockNumber < 0 || blockNumber >= int64(d.numBlocks) {
		return 0, io.EOF
	}

	// Get where read and seek to it in multigz
	log.Printf("block # = %d @ %d", blockNumber, *d.cursorPos)
	var mgzOffset multigz.Offset
	mgzOffset.Block = d.blockStarts[blockNumber]
	mgzOffset.Off = *d.cursorPos % BlockSize
	err := d.in.Seek(mgzOffset)

	// Read stuff
	n, err := d.in.Read(p)

	// If nothing was copied, we EOF'd
	if n == 0 {
		return 0, io.EOF
	}

	// Increment cursor position and return
	*d.cursorPos += int64(n)
	return n, err
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