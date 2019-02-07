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

	"github.com/id01/lz4-go"
	"github.com/golang/snappy"
)

// Compression modes
const (
	GZIP_STORE = iota
	GZIP_MIN = iota
	GZIP_DEFAULT = iota
	GZIP_MAX = iota
	XZ_IN_GZ_MIN = iota
	XZ_IN_GZ = iota
	LZ4 = iota
	SNAPPY = iota
)

// Constants
// Compression binaries
const XZCommand = "xz" // Name of xz binary (if available)
// Debug mode
const DEBUG = false

// Struct containing configurable variables (what used to be constants)
type Compression struct {
	CompressionMode int // Compression mode
	BlockSize uint32 // Size of blocks. Higher block size means better compression but more download bandwidth needed for small downloads
			 // ~1MB is recommended for xz, while ~128KB is recommended for gzip and lz4
	HeuristicBytes int64 // Bytes to perform gzip heuristic on to determine whether a file should be compressed
	NumThreads int // Number of threads to use for compression
	MaxCompressionRatio float64 // Maximum compression ratio for a file to be considered compressible
	BinPath string // Path to compression binary. This is used for all non-gzip compression.
}

// Create a Compression object with a preset mode/bs
func NewCompressionPreset(preset string) (*Compression, error) {
	switch preset {
		case "gzip-store": return NewCompression(GZIP_STORE, 131070) // GZIP-store (dummy) compression
		case "lz4": return NewCompression(LZ4, 262140) // LZ4 compression (very fast)
		case "snappy": return NewCompression(SNAPPY, 262140) // Snappy compression (like LZ4, but slower and worse)
		case "gzip-min": return NewCompression(GZIP_MIN, 131070) // GZIP-min compression (fast)
		case "gzip-default": return NewCompression(GZIP_DEFAULT, 131070) // GZIP-default compression (medium)
		case "xz-min": return NewCompression(XZ_IN_GZ_MIN, 524288) // XZ-min compression (slow)
		case "xz-default": return NewCompression(XZ_IN_GZ, 1048576) // XZ-default compression (very slow)
	}
	return nil, errors.New("Compression mode doesn't exist")
}

// Create a Compression object with some default configuration values
func NewCompression(mode int, bs uint32) (*Compression, error) {
	return NewCompressionAdvanced(mode, bs, 1048576, 12, 0.9)
}

// Create a Compression object
func NewCompressionAdvanced(mode int, bs uint32, hb int64, threads int, mcr float64) (c *Compression, err error) {
	// Set vars
	c = new(Compression)
	c.CompressionMode = mode
	c.BlockSize = bs
	c.HeuristicBytes = hb
	c.NumThreads = threads
	c.MaxCompressionRatio = mcr
	// Get binary path if needed
	err = nil
	if mode == XZ_IN_GZ || mode == XZ_IN_GZ_MIN {
		c.BinPath, err = exec.LookPath(XZCommand)
	}
	return c, err
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
		case GZIP_MAX: return ".gz"
		case XZ_IN_GZ_MIN: fallthrough
		case XZ_IN_GZ: return ".xzgz"
		case LZ4: return ".lz4"
		case SNAPPY: return ".snap"
	}
	panic("Compression mode doesn't exist")
}
// Gets a file extension along with compressibility of file
func (c* Compression) GetFileCompressionInfo(reader io.Reader) (compressable bool, extension string, err error) {
	// Use our compression algorithm to do a heuristic on the first few bytes
	var emulatedBlock, emulatedBlockCompressed bytes.Buffer
	_, err = io.CopyN(&emulatedBlock, reader, c.HeuristicBytes)
	if err != nil {
		return false, "", err
	}
	compressedSize, uncompressedSize, err := c.compressBlock(emulatedBlock.Bytes(), &emulatedBlockCompressed)
	if err != nil {
		return false, "", err
	}
	compressionRatio := float64(compressedSize) / float64(uncompressedSize)

	// If the data is not compressible, return so
	if compressionRatio > c.MaxCompressionRatio {
		return false, ".bin", nil
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
func (c *Compression) compressBlockGz(in []byte, out io.Writer, compressionLevel int) (compressedSize uint32, uncompressedSize int64, err error) {
	// Initialize buffer
	bufw := bufio.NewWriterSize(out, int(c.maxCompressedBlockSize()))

	// Initialize block writer
	outw, err := gzip.NewWriterLevel(bufw, compressionLevel)
	if err != nil {
		return 0, 0, err
	}

	// Compress block
	outw.Write(in)

	// Finalize gzip file, flush buffer and return
	outw.Close()
	blockSize := uint32(bufw.Buffered())
	bufw.Flush()

	return blockSize, int64(len(in)), err
}

// Function that compresses a block using lz4
func (c *Compression) compressBlockLz4(in []byte, out io.Writer) (compressedSize uint32, uncompressedSize int64, err error) {
	// Compress and return
	outBytes, err := lz4.LZ4_compressFrame(in)
	out.Write(outBytes)
	return uint32(len(outBytes)), int64(len(in)), err
}

// Function that compresses a block using snappy
func (c *Compression) compressBlockSnappy(in []byte, out io.Writer) (compressedSize uint32, uncompressedSize int64, err error) {
	// Compress and return
	outBytes := snappy.Encode(nil, in)
	_, err = out.Write(outBytes)
	return uint32(len(outBytes)), int64(len(in)), err
}

// Function that compresses a block using a shell command without wrapping in gzip. Requires an binary corresponding with the command.
func (c *Compression) compressBlockExecNogz(in []byte, out io.Writer, binaryPath string, args []string) (compressedSize uint32, uncompressedSize int64, err error) {
	// Initialize compression subprocess
	subprocess := exec.Command(binaryPath, args...)
	stdin, err := subprocess.StdinPipe()
	if err != nil {
		return 0, 0, err
	}

	// Run subprocess that creates compressed file
	go func() {
		stdin.Write(in)
		stdin.Close()
	}()

	// Get output
	output, err := subprocess.Output()
	if err != nil {
		return 0, 0, err
	}

	// Copy over and return
	n, err := io.Copy(out, bytes.NewReader(output))

	return uint32(n), int64(len(in)), err
}

// Function that compresses a block using a shell command. Requires an binary corresponding with the command.
func (c *Compression) compressBlockExecGz(in []byte, out io.Writer, binaryPath string, args []string) (compressedSize uint32, uncompressedSize int64, err error) {
	reachedEOF := false

	// Compress without gzip wrapper
	var b bytes.Buffer
	_, n, err := c.compressBlockExecNogz(in, &b, binaryPath, args)
	if err == io.EOF {
		reachedEOF = true
	} else if err != nil {
		return 0, n, err
	}

	// Store in gzip and return
	blockSize, _, err := c.compressBlockGz(b.Bytes(), out, 0)
	if reachedEOF == true && err == nil {
		err = io.EOF
	}
	return blockSize, n, err
}

// Wrapper function to compress a block
func (c* Compression) compressBlock(in []byte, out io.Writer) (compressedSize uint32, uncompressedSize int64, err error) {
	switch c.CompressionMode { // Select compression function (and arguments) based on compression mode
		case GZIP_STORE: return c.compressBlockGz(in, out, 0)
		case GZIP_MIN: return c.compressBlockGz(in, out, 1)
		case GZIP_DEFAULT: return c.compressBlockGz(in, out, 6)
		case GZIP_MAX: return c.compressBlockGz(in, out, 9)
		case XZ_IN_GZ: return c.compressBlockExecGz(in, out, XZCommand, []string{"-c"})
		case XZ_IN_GZ_MIN: return c.compressBlockExecGz(in, out, XZCommand, []string{"-c1"})
		case LZ4: return c.compressBlockLz4(in, out)
		case SNAPPY: return c.compressBlockSnappy(in, out)
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
			go func(i int, in []byte){
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
			}(i, inputBuffer.Bytes())
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
				if DEBUG {
					log.Printf("%d %d\n", res.n, res.blockSize)
				}

				// Append block size to block data
				blockData = append(blockData, uint32ToBytes(res.blockSize)...)

				// If this is the last block, add the raw size of the last block to the end of blockData and break
				if eofAt == i {
					if DEBUG {
						log.Printf("%d %d %d\n", res.n, byte(res.n%256), byte(res.n/256))
					}
					blockData = append(blockData, uint32ToBytes(uint32(res.n))...)
					break
				}
			}
		}

		// Get number of bytes written in this block (they should all be in the bufio buffer), then close gzip and flush buffer
		bufw.Flush()

		// If eof happened, break
		if eofAt != -1 {
			if DEBUG {
				log.Printf("%d", eofAt)
			}
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

// Utility function to decompress a block using snappy
func decompressBlockSnappy(in io.Reader, out io.Writer) (n int, err error) {
	var b bytes.Buffer
	io.Copy(&b, in)
	decompressed, err := snappy.Decode(nil, b.Bytes())
	out.Write(decompressed)
	return len(decompressed), err
}

// Utility function to decompress a block using LZ4
func (d *Decompressor) decompressBlockLz4(in io.Reader, out io.Writer) (n int, err error) {
	var b bytes.Buffer
	io.Copy(&b, in)
	decompressed, err := lz4.LZ4_decompressFrame(b.Bytes(), int64(d.c.BlockSize))
	out.Write(decompressed)
	return len(decompressed), err
}

// Utility function to decompress a block range using a shell command which wasn't wrapped in gzip
func decompressBlockRangeExecNogz(in io.Reader, out io.Writer, binaryPath string, args []string) (n int, err error) {
	// Decompress actual compression
	// Initialize decompression subprocess
	subprocess := exec.Command(binaryPath, args...)
	stdin, err := subprocess.StdinPipe()
	if err != nil {
		return 0, err
	}

	// Run subprocess that copies over compressed block
	go func() {
		defer stdin.Close()
		io.Copy(stdin, in)
	}()

	// Get output, copy, and return
	output, err := subprocess.Output()
	if err != nil {
		return 0, err
	}
	n64, err := io.Copy(out, bytes.NewReader(output))
	return int(n64), err
}

// Utility function to decompress a block range using a shell command
func decompressBlockRangeExecGz(in io.Reader, out io.Writer, binaryPath string, args []string) (n int, err error) {
	// "Decompress" gzip (this should be in store mode)
	var b bytes.Buffer
	_, err = decompressBlockRangeGz(in, &b)
	if err != nil {
		return 0, err
	}

	// Decompress actual compression
	return decompressBlockRangeExecNogz(&b, out, binaryPath, args)
}

// Wrapper function to decompress a block range
func (d *Decompressor) decompressBlockRange(in io.Reader, out io.Writer) (n int, err error) {
	switch d.c.CompressionMode { // Select decompression function based off compression mode
		case GZIP_STORE: fallthrough
		case GZIP_MIN: fallthrough
		case GZIP_DEFAULT: fallthrough
		case GZIP_MAX: return decompressBlockRangeGz(in, out)
		case XZ_IN_GZ_MIN: return decompressBlockRangeExecGz(in, out, XZCommand, []string{"-dc1"})
		case XZ_IN_GZ: return decompressBlockRangeExecGz(in, out, XZCommand, []string{"-dc"})
		case LZ4: return d.decompressBlockLz4(in, out)
		case SNAPPY: return decompressBlockSnappy(in, out)
	}
	panic("Compression mode doesn't exist") // If none of the above returned
}

// Wrapper function for decompressBlockRange that implements a cache and multithreading
const CacheSize = 32 // Number of blocks to cache
// Result of decompressing a block
type DecompressionResult struct {
	buffer *bytes.Buffer
//	bytesCopied int
	cacheHit bool
}
func (d *Decompressor) decompressBlockRangeCached(in io.Reader, out io.Writer, startingBlock uint32) (n int, err error) {
	// First, use bufio.Reader to reduce the number of reads and bufio.Writer to reduce the number of writes
	bufin := bufio.NewReader(in)
	bufout := bufio.NewWriter(out)

	// Decompress each block individually if they aren't cached. If they aren't cached, append them to the cache and shift the cache left one.
	currBatch := startingBlock // Block # of start of current batch of blocks
	totalBytesCopied := 0
	for {
		// Loop through threads
		eofAt := -1
		decompressionResults := make([]chan DecompressionResult, d.c.NumThreads)

		for i := 0; i < d.c.NumThreads; i++ {
			// Get currBlock
			currBlock := currBatch + uint32(i)

			// Create channel
			decompressionResults[i] = make(chan DecompressionResult)

			// Check if we've reached EOF
			if currBlock >= d.numBlocks {
//				return totalBytesCopied, nil
				eofAt = i
				break
			}

			// Get block to decompress
			var compressedBlock bytes.Buffer
			var err error
			n, err := io.CopyN(&compressedBlock, bufin, d.blockStarts[currBlock+1]-d.blockStarts[currBlock])
			if err != nil || n == 0 { // End of stream
				eofAt = i
				break
			}

			// Spawn thread to decompress block
			if DEBUG {
				log.Printf("Spawning %d", i)
			}
			go func(i int, currBlock uint32, in io.Reader) {
				var block bytes.Buffer
				var res DecompressionResult

				// Search through cache to see if we have the block cached already
				for x := 0; x < CacheSize; x++ {
					if d.blockCache[x].number == currBlock { // Cache hit
						if DEBUG {
							log.Printf("HIT@%d", d.blockCache[x].number)
						}
						// Get data from cache
						io.Copy(&block, bytes.NewReader(d.blockCache[x].data))
						// Discard data from input
//						io.Copy(ioutil.Discard, in)
						// That's all. Return to our chan
						res.buffer = &block
//						res.bytesCopied = n
						res.cacheHit = true
						decompressionResults[i] <- res
						return
					}
				}

				// If we had a cache miss, decompress the block
				if DEBUG {
					log.Printf("MISS@%d", currBlock)
				}
				// Decompress block
				d.decompressBlockRange(in, &block)
				res.buffer = &block
//				res.bytesCopied = n
				res.cacheHit = false
				decompressionResults[i] <- res
				return
			}(i, currBlock, &compressedBlock)
		}
		if DEBUG {
			log.Printf("Eof at %d", eofAt)
		}

		// Process results
		for i := 0; i < d.c.NumThreads; i++ {
			// If we got EOF, return
			if eofAt == i {
				return totalBytesCopied, nil
			}

			// Get result and close
			res := <- decompressionResults[i]
			close(decompressionResults[i])

			// Copy to output and add to total bytes copied
			n, _ := io.Copy(bufout, res.buffer)
			totalBytesCopied += int(n)

			// If we got a cache miss, move everything in the cache left one, then put this at the end
			if !res.cacheHit {
				currBlock := currBatch + uint32(i)
				for x := 1; x < CacheSize; x++ {
					d.blockCache[x-1] = d.blockCache[x]
				}
				var newEntry CacheBlock
				newEntry.number = currBlock
				newEntry.data = res.buffer.Bytes()
				d.blockCache[CacheSize-1] = newEntry
			}
		}

		// Add NumThreads to currBatch
//		currBlock++
		currBatch += uint32(d.c.NumThreads)
	}
}

/*** MAIN DECOMPRESSION INTERFACE ***/
// Cache representation of a block
type CacheBlock struct {
	number uint32 // Block number
	data []byte // Data of the block (uncompressed)
}

// ReadSeeker implementation for decompression
type Decompressor struct {
	cursorPos *int64		// The current location we have seeked to
	blockStarts []int64		// The start of each block. These will be recovered from the block sizes
	blockCache []CacheBlock		// Cached blocks. Used to improve performance.
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
	if DEBUG {
		log.Printf("size = %d, gzippedBlockDataLen = %d\n", size, gzippedBlockDataLen)
	}
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
		if DEBUG {
			log.Printf("%d", gzipExtraDataLen)
		}
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
	if blockDataLen%4 != 0 {
		return errors.New("Length of block data should be a multiple of 4; file may be corrupted")
	}
	d.numBlocks = uint32((blockDataLen-4)/4)
	if DEBUG {
		log.Printf("metadata len, numblocks = %d, %d", blockDataLen, d.numBlocks)
	}
	d.blockStarts = make([]int64, d.numBlocks+1) // Starts with 0, ends with end of last block (and beginning of metadata)
	currentBlockPosition := int64(0)
	for i := uint32(0); i < d.numBlocks; i++ { // Loop through block data, getting starts of blocks.
		bs := i*4 // Location of start of data for our current block
		d.blockStarts[i] = currentBlockPosition // Note: Remember that the first entry can be anything now, but we're making the first
							// of this array still always 0 for easier indexing
		currentBlockSize := bytesToUint32(blockData[bs:bs+4])
		currentBlockPosition += int64(currentBlockSize) // Note: We increase the current block position after
							// recording the size (the size is for the current block this time, though)
	}
	d.blockStarts[d.numBlocks] = currentBlockPosition // End of last block (and beginning of metadata)

	//log.Printf("Block Starts: %v\n", d.blockStarts)

	// Get uncompressed size of last block and derive uncompressed size of file
	lastBlockRawSize := bytesToUint32(blockData[blockDataLen-4:])
	d.decompressedSize = int64(d.numBlocks-1) * int64(d.c.BlockSize) + int64(lastBlockRawSize)
	if DEBUG {
		log.Printf("Decompressed size = %d", d.decompressedSize)
	}

	// Initialize cursor position and copy over reader
	*d.cursorPos = 0
	in.Seek(0, io.SeekStart)
	d.in = in

	// Initialize block cache and return
	d.blockCache = make([]CacheBlock, CacheSize)
	for i := 0; i < CacheSize; i++ {
		d.blockCache[i].number = 0xffffffff // There should never be this block number
	}

	return nil
}

// Reads data using a decompressor
func (d Decompressor) Read(p []byte) (int, error) {
	if DEBUG {
		log.Printf("Cursor pos before: %d\n", *d.cursorPos)
	}
	// Check if we're at the end of the file or before the beginning of the file
	if *d.cursorPos >= d.decompressedSize || *d.cursorPos < 0 {
		if DEBUG {
			log.Println("Out of bounds EOF")
		}
		return 0, io.EOF
	}

	// Get block range to read
	blockNumber := *d.cursorPos / int64(d.c.BlockSize)
	blockStart := d.blockStarts[blockNumber] // Start position of blocks to read
	dataOffset := *d.cursorPos % int64(d.c.BlockSize) // Offset of data to read in blocks to read
	bytesToRead := len(p) // Number of bytes to read
	blocksToRead := (int64(bytesToRead) + dataOffset) / int64(d.c.BlockSize) + 1 // Number of blocks to read
	returnEOF := false
	if blockNumber + blocksToRead > int64(d.numBlocks) { // Overflowed the last block
		blocksToRead = int64(d.numBlocks)-blockNumber
		returnEOF = true
	}
	var blockEnd int64 // End position of blocks to read
	blockEnd = d.blockStarts[blockNumber + blocksToRead] // Start of the block after the last block we want to get is the end of the last block we want to get
	blockLen := blockEnd - blockStart

	// Read compressed block range into buffer
	var compressedBlocks bytes.Buffer
	d.in.Seek(blockStart, io.SeekStart)
	n1, err := io.CopyN(&compressedBlocks, d.in, blockLen)
	if DEBUG {
		log.Printf("block # = %d @ %d <- %d, len %d, copied %d bytes", blockNumber, blockStart, *d.cursorPos, blockLen, n1)
	}
	if err != nil {
		if DEBUG {
			log.Println("Copy Error")
		}
		return 0, err
	}

	// Decompress block range
	var b bytes.Buffer
	n, err := d.decompressBlockRangeCached(&compressedBlocks, &b, uint32(blockNumber))
	if err != nil {
		log.Println("Decompression error")
		return n, err
	}

	// Calculate bytes read
	readOverflow := *d.cursorPos + int64(bytesToRead) - d.decompressedSize
	if readOverflow < 0 {
		readOverflow = 0
	}
	bytesRead := int64(bytesToRead) - readOverflow
	if DEBUG {
		log.Printf("Read offset = %d, overflow = %d", dataOffset, readOverflow)
		log.Printf("Decompressed %d bytes; read %d out of %d bytes\n", n, bytesRead, bytesToRead)
	}

	// If we read 0 bytes, we reached the end of the file
	if bytesRead == 0 {
		log.Println("EOF")
		return 0, io.EOF
	}

	// Copy from buffer+offset to p
	io.CopyN(ioutil.Discard, &b, dataOffset)
	b.Read(p) // Note: everything after bytesToRead bytes will be discarded; we are returning bytesToRead instead of n

	// Increment cursor position and return
	*d.cursorPos += bytesRead
	if returnEOF {
		if DEBUG {
			log.Println("EOF")
		}
		return int(bytesRead), io.EOF
	}
	return int(bytesRead), nil
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
