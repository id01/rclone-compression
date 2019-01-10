Rclone-MGZ  
Code for rclone multigz integration.  
This is a prototype. Weird things may happen.

Configurable constants:
* CompressionLevel: Gzip Compression Level
* BlockSize: Size of each gzip block. Up to 65502 can be indexed in 2 bytes. Currently hard coded to use two bytes per block index.
* InitialChunkLength: Size of the initial chunk to get from the end of the file. If this is large enough, it will not be necessary to re-read metadata.

Structure of file:
* gzip data
* 0xabcd (to make sure this isn't recognized as another gzip file or block)
* block data (consisting of 6 byte precedingSize-checksum combos)
* size of block data (4 bytes)
* fake gzip footer (8 bytes)