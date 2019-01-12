Rclone-compression  
Code for rclone compression integration.  
This is a prototype. Weird things may happen.

Configurable constants are located and explained at the top of the file.

Non-Configurable constants (or variables that act like constants):
* gzipHeaderData in gzipExtraify: The data contained in our gzip header.
	* This is currently configured to allow us an extra data field with no other extra fields.
	* POSIX modification time is locked to 0, and operating system is locked to Linux.
* gzipContentAndFooter in gzipExtraify: The data contained in our gzip content and footer.
	* This is the same as an empty file. CRC-32 checksum and decompressed size are both 0
* GzipHeaderSize: size of gzipHeaderData
* GzipDataAndFooterSize: size of gzipContentAndFooter
* LengthOffsetFromEnd: Offset from end where we can find the size of our gzip files with block data in the extra data fields
* TrailingBytes: Bytes after our block data gzip files

Structure of file:
* gzip data (or gzipped xz data). This is many individual gzip (or gzipped xz) files concatenated into a single stream
* empty gzip files containing block data (block data is gzipped into a gzip file then split among extra data fields in empty gzip files)
* empty gzip file containing total size of all block data gzip files
