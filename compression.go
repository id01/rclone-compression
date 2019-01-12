// Note: I am not a go programmer; this may need some refining.
// It's my first time writing golang :)
package press // POC Compressor

import (
	"log"
	"io"
	"io/ioutil"
	"errors"
	"bytes"
	"bufio"
	"compress/gzip"
	"os/exec"
)

// Compression modes
const (
	GZIP_STORE = iota
	GZIP_MIN = iota
	GZIP_DEFAULT = iota
	GZIP_MAX = iota
	XZ_IN_GZ = iota
	LZ4_IN_GZ = iota
)

// Constants
// Compression binary paths
const XZCommand = "/usr/bin/xz" // Path to xz binary (if available)
const LZ4Command = "/usr/bin/lz4" // Path to lz4 binary (if available)

// Struct containing configurable variables (what used to be constants)
type Compression struct {
	CompressionMode int // Compression mode
	BlockSize uint32 // Size of blocks. Higher block size means better compression but more download bandwidth needed for small downloads
			 // ~1MB is recommended for xz, while ~128KB is recommended for gzip and lz4
	HeuristicBytes int64 // Bytes to perform gzip heuristic on to determine whether a file should be compressed
	NumThreads int // Number of threads to use for compression
	MaxCompressionRatio float64 // Maximum compression ratio for a file to be considered compressible
}

// Create a Compression object with some default configuration values
func NewCompression(mode int, bs uint32) *Compression {
	return NewCompressionAdvanced(mode, bs, 1048576, 6, 0.9)
}

// Create a Compression object
func NewCompressionAdvanced(mode int, bs uint32, hb int64, threads int, mcr float64) *Compression {
	c := new(Compression)
	c.CompressionMode = mode
	c.BlockSize = bs
	c.HeuristicBytes = hb
	c.NumThreads = threads
	c.MaxCompressionRatio = mcr
	return c
}

/*** UTILITY FUNCTIONS ***/
// Gets an overestimate for the maximum compressed block size
func (c* Compression) maxCompressedBlockSize() uint32 {
	return c.BlockSize + (c.BlockSize>>2) + 256
}

// Gets file extension for current compression mode
func (c* Compression) GetFileExtension() string {
	switch c.CompressionMode {
		case GZIP_STORE: fallthrough
		case GZIP_MIN: fallthrough
		case GZIP_DEFAULT: fallthrough
		case GZIP_MAX: return ".bin.gz"
		case XZ_IN_GZ: return ".xz.gz"
		case LZ4_IN_GZ: return ".lz4.gz"
	}
	panic("Compression mode doesn't exist")
}
// Gets a file extension along with compressibility of file
func (c* Compression) GetFileCompressionInfo(reader io.Reader) (compressable bool, extension string, err error) {
	// Use gzip-min to do a fast heuristic on the first few bytes
	var emulatedBlock, emulatedBlockCompressed bytes.Buffer
	_, err = io.CopyN(&emulatedBlock, reader, c.HeuristicBytes)
	if err != nil {
		return false, "", err
	}
	compressedSize, uncompressedSize, err := c.compressBlockGz(&emulatedBlock, &emulatedBlockCompressed, 1)
	if err != nil {
		return false, "", err
	}
	compressionRatio := float64(compressedSize) / float64(uncompressedSize)

	// If the data is not compressible, return so
	if compressionRatio > c.MaxCompressionRatio {
		return false, ".bin.bin", nil
	}

	// If the file is compressible, select file extension based on compression mode
	return true, c.GetFileExtension(), nil
}

/*** BYTE CONVERSION FUNCTIONS ***/
// Converts uint16 to bytes (little endian)
func uint16ToBytes(n uint16) []byte {
	return []byte{byte(n&0xff), byte(n>>8)}
}

// Converts bytes to uint16 (little endian)
func bytesToUint16(n []byte) uint16 {
	return uint16(n[0])+(uint16(n[1])<<8)
}

// Converts uint32 to bytes (little endian)
func uint32ToBytes(n uint32) []byte {
	return append(uint16ToBytes(uint16(n&0xffff)), uint16ToBytes(uint16(n>>16))...)
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

/*** BLOCK DATA SERIALIZATION FUNCTIONS ***/
// These should be constant
var gzipHeaderData = []byte{0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03} // A gzip header that allows for extra data
var gzipContentAndFooter = []byte{0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00} // Empty gzip content and footer
// Size of gzip header and footer for gzip files that are storing block data in extra data fields
const GzipHeaderSize = 10
const GzipDataAndFooterSize = 10
// Splits data into extra data in empty gzip files, followed by a gzip file storing the total length of all the prior gzip files as a uint32
func gzipExtraify(in io.Reader, out io.Writer) {
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

/*** BLOCK COMPRESSION FUNCTIONS ***/
// Function that compresses a block using gzip
func (c *Compression) compressBlockGz(in io.Reader, out io.Writer, compressionLevel int) (compressedSize uint32, uncompressedSize int64, err error) {
	// Initialize buffer
	bufw := bufio.NewWriterSize(out, int(c.maxCompressedBlockSize()))

	// Initialize block writer
	outw, err := gzip.NewWriterLevel(bufw, compressionLevel)
	if err != nil {
		return 0, 0, err
	}

	// Compress block
	n, err := io.Copy(outw, in)

	// Finalize gzip file, flush buffer and return
	outw.Close()
	blockSize := uint32(bufw.Buffered())
	bufw.Flush()

	return blockSize, n, err
}

// Function that compresses a block using a shell command. Requires an binary corresponding with the command.
func (c *Compression) compressBlockExec(in io.Reader, out io.Writer, binaryPath string) (compressedSize uint32, uncompressedSize int64, err error) {
	// Initialize compression subprocess
	subprocess := exec.Command(binaryPath, "-c")
	stdin, err := subprocess.StdinPipe()
	if err != nil {
		return 0, 0, err
	}

	// Run subprocess that creates compressed file
	nChan := make(chan int64)
	go func() {
		n, _ := io.Copy(stdin, in)
		stdin.Close()
		nChan <- n
	}()

	// Get output
	output, err := subprocess.Output()
	if err != nil {
		return 0, 0, err
	}

	// Store in gzip and return
	blockSize, _, err := c.compressBlockGz(bytes.NewReader(output), out, 0)

	return blockSize, <-nChan, err
}

// Wrapper function to compress a block
func (c* Compression) compressBlock(in io.Reader, out io.Writer) (compressedSize uint32, uncompressedSize int64, err error) {
	switch c.CompressionMode { // Select compression function (and arguments) based on compression mode
		case GZIP_STORE: return c.compressBlockGz(in, out, 0)
		case GZIP_MIN: return c.compressBlockGz(in, out, 1)
		case GZIP_DEFAULT: return c.compressBlockGz(in, out, 6)
		case GZIP_MAX: return c.compressBlockGz(in, out, 9)
		case XZ_IN_GZ: return c.compressBlockExec(in, out, XZCommand)
		case LZ4_IN_GZ: return c.compressBlockExec(in, out, LZ4Command)
	}
	panic("Compression mode doesn't exist")
}

/*** MAIN COMPRESSION INTERFACE ***/
// Result of compression for a single block (gotten by a single thread)
type CompressionResult struct {
	buffer *bytes.Buffer
	blockSize uint32
	n int64
	err error
}

// Compresses a file. Argument "size" is ignored.
func (c *Compression) CompressFile(in io.Reader, size int64, out io.Writer) error {
	// Initialize buffered writer
	bufw := bufio.NewWriterSize(out, int(c.maxCompressedBlockSize()*uint32(c.NumThreads)))

	// Write gzip
	var blockData []byte = make([]byte, 0)
	for {
		// Loop through threads, spawning a go procedure for each thread. If we get eof on one thread, set eofAt to that thread and break
		compressionResults := make([]chan CompressionResult, c.NumThreads)
		eofAt := -1
		for i := 0; i < c.NumThreads; i++ {
			// Create thread channel and allocate buffer to pass to thread
			compressionResults[i] = make(chan CompressionResult)
			var inputBuffer bytes.Buffer
			_, err := io.CopyN(&inputBuffer, in, int64(c.BlockSize))
			if err == io.EOF {
				eofAt = i
			} else if err != nil {
				return err
			}
			// Run thread
			go func(i int, in io.Reader, bufw io.Writer){
				// Initialize thread writer and result struct
				var res CompressionResult
				var buffer bytes.Buffer

				// Compress block
				blockSize, n, err := c.compressBlock(in, &buffer)
				if err != nil && err != io.EOF { // This errored out.
					res.buffer = nil
					res.blockSize = 0
					res.n = 0
					res.err = err
					compressionResults[i] <- res
					return
				}
				// Pass our data back to the main thread as a compression result
				res.buffer = &buffer
				res.blockSize = blockSize
				res.n = n
				res.err = err
				compressionResults[i] <- res
				return
			}(i, &inputBuffer, bufw)
			// If we have reached eof, we don't need more threads
			if eofAt != -1 {
				break
			}
		}

		// Process writers in order
		for i := 0; i < c.NumThreads; i++ {
			if compressionResults[i] != nil {
				// Get current compression result, get buffer, and copy buffer over to output
				res := <-compressionResults[i]
				close(compressionResults[i])
				if res.buffer == nil {
					return res.err
				}
				io.Copy(bufw, res.buffer)
				log.Printf("%d %d\n", res.n, res.blockSize)

				// Append block size to block data
				blockData = append(blockData, uint32ToBytes(res.blockSize)...)

				// If this is the last block, add the raw size of the last block to the end of blockData and break
				if eofAt == i {
					log.Printf("%d %d %d\n", res.n, byte(res.n%256), byte(res.n/256))
					blockData = append(blockData, uint32ToBytes(uint32(res.n))...)
					break
				}
			}
		}

		// Get number of bytes written in this block (they should all be in the bufio buffer), then close gzip and flush buffer
		bufw.Flush()

		// If eof happened, break
		if eofAt != -1 {
			log.Printf("%d", eofAt)
			break
		}
	}

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

	// Append extra data gzips to end of bufw, then flush bufw
	gzipExtraify(bytes.NewReader(b.Bytes()), out)
	bufw.Flush()

	// Return success
	return nil
}

/*** BLOCK DECOMPRESSION FUNCTIONS ***/
// Utility function to decompress a block range using gzip
func decompressBlockRangeGz(in io.Reader, out io.Writer) (n int, err error) {
	gzipReader, err := gzip.NewReader(in)
	if err != nil {
		return 0, err
	}
	written, err := io.Copy(out, gzipReader)
	return int(written), err
}

// Utility function to decompress a block range using a shell command
func decompressBlockRangeExec(in io.Reader, out io.Writer, binaryPath string) (n int, err error) {
	// "Decompress" gzip (this should be in store mode)
	var b bytes.Buffer
	_, err = decompressBlockRangeGz(in, &b)
	if err != nil {
		return 0, err
	}

	// Decompress actual compression
	// Initialize decompression subprocess
	subprocess := exec.Command(binaryPath, "-dc")
	stdin, err := subprocess.StdinPipe()
	if err != nil {
		return 0, err
	}

	// Run subprocess that copies over compressed block
	go func() {
		defer stdin.Close()
		io.Copy(stdin, &b)
	}()

	// Get output, copy, and return
	output, err := subprocess.Output()
	if err != nil {
		return 0, err
	}
	n64, err := io.Copy(out, bytes.NewReader(output))
	return int(n64), err
}

// Wrapper function to decompress a block range
func (d *Decompressor) decompressBlockRange(in io.Reader, out io.Writer) (n int, err error) {
	switch d.c.CompressionMode { // Select decompression function based off compression mode
		case GZIP_STORE: fallthrough
		case GZIP_MIN: fallthrough
		case GZIP_DEFAULT: fallthrough
		case GZIP_MAX: return decompressBlockRangeGz(in, out)
		case XZ_IN_GZ: return decompressBlockRangeExec(in, out, XZCommand)
		case LZ4_IN_GZ: return decompressBlockRangeExec(in, out, LZ4Command)
	}
	panic("Compression mode doesn't exist") // If none of the above returned
}

/*** MAIN DECOMPRESSION INTERFACE ***/
// ReadSeeker implementation for decompression
type Decompressor struct {
	cursorPos *int64		// The current location we have seeked to
	blockStarts []int64		// The start of each block. These will be recovered from the block sizes
	numBlocks uint32		// Number of blocks
	decompressedSize int64		// Decompressed size of the file.
	in io.ReadSeeker		// Input
	c *Compression			// Compression options
}

// Decompression constants
const LengthOffsetFromEnd = GzipDataAndFooterSize+4 // How far the 4-byte length of gzipped data is from the end
const TrailingBytes = LengthOffsetFromEnd+2+GzipHeaderSize // This is the total size of the last gzip file in the stream, which is not included in the length of gzipped data

// Initializes decompressor. Takes 3 reads. Works best with cached ReadSeeker.
func (d* Decompressor) init(c *Compression, in io.ReadSeeker, size int64) error {
	// Copy over compression
	d.c = c

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
	d.decompressedSize = int64((d.numBlocks-1) * d.c.BlockSize + uint32(lastBlockRawSize))

	// Initialize cursor position and copy over reader
	*d.cursorPos = 0
	in.Seek(0, io.SeekStart)
	d.in = in

	return nil
}

// Reads data using a decompressor
func (d Decompressor) Read(p []byte) (int, error) {	
	log.Printf("Cursor pos before: %d\n", *d.cursorPos)
	// Check if we're at the end of the file or before the beginning of the file
	if *d.cursorPos >= d.decompressedSize || *d.cursorPos < 0 {
		log.Println("Out of bounds EOF")
		return 0, io.EOF
	}

	// Get block range to read
	blockNumber := *d.cursorPos / int64(d.c.BlockSize)
	blockStart := d.blockStarts[blockNumber] // Start position of blocks to read
	dataOffset := *d.cursorPos % int64(d.c.BlockSize) // Offset of data to read in blocks to read
	bytesToRead := len(p) // Number of bytes to read
	blocksToRead := (int64(bytesToRead) + dataOffset) / int64(d.c.BlockSize) + 1 // Number of blocks to read
	var blockEnd int64 // End position of blocks to read
	if blockNumber + blocksToRead < int64(d.numBlocks) {
		blockEnd = d.blockStarts[blockNumber + blocksToRead] // Start of the next block after the last block is the end of the last block
	} else {
		blockEnd = d.blockStarts[d.numBlocks - 1] + int64(d.c.maxCompressedBlockSize()) // Always off the end of the last block
	}
	blockLen := blockEnd - blockStart
	log.Printf("block # = %d @ %d, len %d", blockNumber, *d.cursorPos, blockLen)

	// Read compressed block range into buffer
	d.in.Seek(blockStart, io.SeekStart)
	compressedBlocks := make([]byte, blockLen)
	n, err := d.in.Read(compressedBlocks)
	if err != nil {
		return 0, err
	}
	log.Printf("Copied %d bytes from %d\n", n, blockStart)

	// If we reached the end of the file, trim compressedBlocks to first n bytes
	if int64(n) != blockLen {
		compressedBlocks = compressedBlocks[:n]
	}

	// Decompress block range
	var b bytes.Buffer
	n, err = d.decompressBlockRange(bytes.NewReader(compressedBlocks), &b)
	if err != nil {
		return n, err
	}

	// Calculate bytes read
	readOverflow := *d.cursorPos + int64(bytesToRead) - d.decompressedSize
	if readOverflow < 0 {
		readOverflow = 0
	}
	log.Printf("Read offset = %d, overflow = %d", dataOffset, readOverflow)
	bytesRead := int64(bytesToRead) - readOverflow
	log.Printf("Decompressed %d bytes; read %d out of %d bytes\n", n, bytesRead, bytesToRead)

	// If we read 0 bytes, we reached the end of the file
	if bytesRead == 0 {
		return 0, io.EOF
	}

	// Copy from buffer+offset to p
	io.CopyN(ioutil.Discard, &b, dataOffset)
	b.Read(p) // Note: everything after bytesToRead bytes will be discarded; we are returning bytesToRead instead of n

	// Increment cursor position and return
	*d.cursorPos += bytesRead
	return int(bytesRead), err
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
func (c *Compression) DecompressFile(in io.ReadSeeker, size int64) (FileHandle io.ReadSeeker, decompressedSize int64, err error) {
	var decompressor Decompressor
	err = decompressor.init(c, in, size)
	return decompressor, decompressor.decompressedSize, err
}
