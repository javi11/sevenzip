package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/javi11/sevenzip"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// DirectExtractor demonstrates direct extraction from 7zip files using offset metadata
// This shows how to extract files WITHOUT using the sevenzip library's extraction methods,
// only using the metadata from ListFilesWithOffsets
type DirectExtractor struct {
	archivePath string
	volumes     []string
	volumeSizes []int64
	password    string
}

// NewDirectExtractor creates a new direct extractor
// It only reads metadata from the archive, not the actual file data
func NewDirectExtractor(archivePath, password string) (*DirectExtractor, error) {
	de := &DirectExtractor{
		archivePath: archivePath,
		password:    password,
	}

	// Determine if this is a multi-volume archive
	if filepath.Ext(archivePath) == ".001" {
		// Multi-volume archive - find all parts
		base := archivePath[:len(archivePath)-4]
		for i := 1; ; i++ {
			volPath := fmt.Sprintf("%s.%03d", base, i)
			info, err := os.Stat(volPath)
			if err != nil {
				if os.IsNotExist(err) {
					break
				}
				return nil, fmt.Errorf("failed to stat volume %s: %w", volPath, err)
			}
			de.volumes = append(de.volumes, volPath)
			de.volumeSizes = append(de.volumeSizes, info.Size())
		}
	} else {
		// Single volume
		info, err := os.Stat(archivePath)
		if err != nil {
			return nil, fmt.Errorf("failed to stat archive: %w", err)
		}
		de.volumes = []string{archivePath}
		de.volumeSizes = []int64{info.Size()}
	}

	if len(de.volumes) == 0 {
		return nil, fmt.Errorf("no archive volumes found")
	}

	return de, nil
}

// Close is a no-op since we don't keep files open
func (de *DirectExtractor) Close() error {
	return nil
}

// ExtractFileByOffset extracts a file using direct offset reading
// This only works for uncompressed, non-encrypted files
func (de *DirectExtractor) ExtractFileByOffset(fileInfo sevenzip.FileInfo, outputPath string) error {
	if fileInfo.Compressed || fileInfo.Encrypted {
		return fmt.Errorf("file %s is compressed or encrypted, cannot use direct extraction", fileInfo.Name)
	}

	fmt.Printf("\nDirect extraction of: %s\n", fileInfo.Name)
	fmt.Printf("  Offset: %d bytes\n", fileInfo.Offset)
	fmt.Printf("  Size: %d bytes\n", fileInfo.Size)

	// Calculate which volume(s) contain this file
	volumeIndex, volumeOffset := de.calculateVolumePosition(fileInfo.Offset)

	fmt.Printf("  Starting in volume %d at offset %d\n", volumeIndex+1, volumeOffset)

	// Open the output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Read the file data from the archive
	bytesToRead := int64(fileInfo.Size)
	totalRead := int64(0)

	for bytesToRead > 0 && volumeIndex < len(de.volumes) {
		// Open the current volume
		volume, err := os.Open(de.volumes[volumeIndex])
		if err != nil {
			return fmt.Errorf("failed to open volume %s: %w", de.volumes[volumeIndex], err)
		}

		// Seek to the offset in this volume
		_, err = volume.Seek(volumeOffset, io.SeekStart)
		if err != nil {
			volume.Close()
			return fmt.Errorf("failed to seek in volume: %w", err)
		}

		// Calculate how much we can read from this volume
		remainingInVolume := de.volumeSizes[volumeIndex] - volumeOffset
		readSize := bytesToRead
		if readSize > remainingInVolume {
			readSize = remainingInVolume
		}

		// Read from the volume
		written, err := io.CopyN(outFile, volume, readSize)
		volume.Close()

		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read from volume: %w", err)
		}

		totalRead += written
		bytesToRead -= written

		// Move to the next volume if needed
		volumeIndex++
		volumeOffset = 0 // Start at the beginning of the next volume
	}

	if totalRead != int64(fileInfo.Size) {
		return fmt.Errorf("read size mismatch: expected %d, got %d", fileInfo.Size, totalRead)
	}

	fmt.Printf("  Successfully extracted %d bytes to %s\n", totalRead, outputPath)

	return nil
}

// calculateVolumePosition determines which volume contains the given offset
// and the offset within that volume
func (de *DirectExtractor) calculateVolumePosition(globalOffset int64) (volumeIndex int, localOffset int64) {
	currentOffset := int64(0)

	for i, size := range de.volumeSizes {
		if globalOffset < currentOffset+size {
			return i, globalOffset - currentOffset
		}
		currentOffset += size
	}

	// If we get here, the offset is beyond all volumes
	return len(de.volumes) - 1, globalOffset - currentOffset
}

// aesDecoderReader implements a streaming AES-CBC decoder
// Similar to https://github.com/nzbdav-dev/nzbdav/blob/main/backend/Streams/AesDecoderStream.cs
type aesDecoderReader struct {
	source      io.Reader
	cipher      cipher.BlockMode
	buf         []byte   // buffer for incomplete blocks
	eof         bool
	blockSize   int
	totalSize   uint64 // expected unencrypted size
	bytesRead   uint64
}

// newAESDecoderReader creates a new streaming AES decoder
func newAESDecoderReader(source io.Reader, key, iv []byte, unencryptedSize uint64) (*aesDecoderReader, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	return &aesDecoderReader{
		source:    source,
		cipher:    cipher.NewCBCDecrypter(block, iv),
		buf:       make([]byte, 0, aes.BlockSize*16), // buffer for up to 16 blocks
		blockSize: aes.BlockSize,
		totalSize: unencryptedSize,
	}, nil
}

// Read implements io.Reader for streaming AES decryption
func (r *aesDecoderReader) Read(p []byte) (n int, err error) {
	if r.bytesRead >= r.totalSize {
		return 0, io.EOF
	}

	// Calculate how much decrypted data we need
	remaining := r.totalSize - r.bytesRead
	if uint64(len(p)) > remaining {
		p = p[:remaining]
	}

	// Try to fill p with decrypted data
	for len(p) > 0 && !r.eof {
		// If we have buffered decrypted data, use it first
		if len(r.buf) > 0 {
			copied := copy(p, r.buf)
			p = p[copied:]
			r.buf = r.buf[copied:]
			n += copied
			r.bytesRead += uint64(copied)

			if len(p) == 0 || r.bytesRead >= r.totalSize {
				return n, nil
			}
		}

		// Read and decrypt more blocks
		encrypted := make([]byte, r.blockSize*16) // read multiple blocks at once
		bytesRead, readErr := r.source.Read(encrypted)

		if bytesRead > 0 {
			// Decrypt only complete blocks
			completeBlocks := (bytesRead / r.blockSize) * r.blockSize
			if completeBlocks > 0 {
				decrypted := make([]byte, completeBlocks)
				r.cipher.CryptBlocks(decrypted, encrypted[:completeBlocks])
				r.buf = append(r.buf, decrypted...)
			}
		}

		if readErr == io.EOF {
			r.eof = true
			if len(r.buf) == 0 {
				if n > 0 {
					return n, nil
				}
				return 0, io.EOF
			}
		} else if readErr != nil {
			return n, readErr
		}
	}

	if n == 0 && r.eof {
		return 0, io.EOF
	}

	return n, nil
}

// deriveAESKey derives the AES encryption key from a password using the 7-zip algorithm
func deriveAESKey(password string, fileInfo sevenzip.FileInfo) ([]byte, error) {
	// Build the input for hashing: salt + password (UTF-16LE)
	b := bytes.NewBuffer(fileInfo.AESSalt)

	// Convert password to UTF-16LE
	utf16le := unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM)
	t := transform.NewWriter(b, utf16le.NewEncoder())
	if _, err := t.Write([]byte(password)); err != nil {
		return nil, fmt.Errorf("failed to encode password: %w", err)
	}

	// Calculate the key using SHA-256
	key := make([]byte, sha256.Size)

	if fileInfo.KDFIterations == 0 {
		// Special case: no hashing, use data directly (padded/truncated to 32 bytes)
		copy(key, b.Bytes())
	} else {
		// Apply SHA-256 hash in rounds
		h := sha256.New()
		for i := uint64(0); i < uint64(fileInfo.KDFIterations); i++ {
			h.Write(b.Bytes())
			binary.Write(h, binary.LittleEndian, i)
		}
		copy(key, h.Sum(nil))
	}

	return key, nil
}

// multiVolumeReader implements io.Reader that can span multiple volume files
type multiVolumeReader struct {
	volumes       []string
	volumeSizes   []int64
	currentVolume int
	currentOffset int64
	totalOffset   int64
	file          *os.File
}

// newMultiVolumeReader creates a reader that can read across multiple volumes
func newMultiVolumeReader(volumes []string, volumeSizes []int64, startOffset int64) (*multiVolumeReader, error) {
	// Find which volume contains the start offset
	currentOffset := int64(0)
	volumeIndex := 0
	localOffset := startOffset

	for i, size := range volumeSizes {
		if startOffset < currentOffset+size {
			volumeIndex = i
			localOffset = startOffset - currentOffset
			break
		}
		currentOffset += size
	}

	// Open the first volume
	file, err := os.Open(volumes[volumeIndex])
	if err != nil {
		return nil, fmt.Errorf("failed to open volume %s: %w", volumes[volumeIndex], err)
	}

	// Seek to the start offset in this volume
	if _, err := file.Seek(localOffset, io.SeekStart); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek in volume: %w", err)
	}

	return &multiVolumeReader{
		volumes:       volumes,
		volumeSizes:   volumeSizes,
		currentVolume: volumeIndex,
		currentOffset: localOffset,
		totalOffset:   startOffset,
		file:          file,
	}, nil
}

// Read implements io.Reader for reading across multiple volumes
func (r *multiVolumeReader) Read(p []byte) (n int, err error) {
	if r.currentVolume >= len(r.volumes) {
		return 0, io.EOF
	}

	// Read from current volume
	bytesRead, err := r.file.Read(p)
	n += bytesRead
	r.currentOffset += int64(bytesRead)
	r.totalOffset += int64(bytesRead)

	// If we hit EOF and there are more volumes, open the next one
	if err == io.EOF && r.currentVolume+1 < len(r.volumes) {
		r.file.Close()
		r.currentVolume++
		r.currentOffset = 0

		// Open next volume
		nextFile, openErr := os.Open(r.volumes[r.currentVolume])
		if openErr != nil {
			return n, fmt.Errorf("failed to open next volume: %w", openErr)
		}
		r.file = nextFile

		// If we didn't read anything yet, try reading from the new volume
		if n == 0 {
			return r.Read(p)
		}

		// Return what we read so far without error
		return n, nil
	}

	return n, err
}

// Close closes the current volume file
func (r *multiVolumeReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// ExtractEncryptedFileByOffset extracts an encrypted file using streaming AES decryption
// This demonstrates reading directly from 7zip file bytes using only offset metadata
func (de *DirectExtractor) ExtractEncryptedFileByOffset(fileInfo sevenzip.FileInfo, outputPath string) error {
	if !fileInfo.Encrypted {
		return fmt.Errorf("file %s is not encrypted", fileInfo.Name)
	}

	if de.password == "" {
		return fmt.Errorf("password required to extract encrypted file %s", fileInfo.Name)
	}

	// Validate AES parameters are present
	if fileInfo.AESIV == nil || len(fileInfo.AESIV) != 16 {
		return fmt.Errorf("invalid or missing AES IV for file %s", fileInfo.Name)
	}

	if fileInfo.KDFIterations == 0 {
		return fmt.Errorf("invalid KDF iterations for file %s", fileInfo.Name)
	}

	fmt.Printf("\nStreaming extraction of encrypted file: %s\n", fileInfo.Name)
	fmt.Printf("  Offset: %d bytes\n", fileInfo.Offset)
	fmt.Printf("  Uncompressed Size: %d bytes\n", fileInfo.Size)
	fmt.Printf("  Packed Size: %d bytes\n", fileInfo.PackedSize)
	fmt.Printf("  Encryption: AES-256-CBC (streaming)\n")
	if len(fileInfo.AESSalt) > 0 {
		fmt.Printf("  Salt: %x (%d bytes)\n", fileInfo.AESSalt, len(fileInfo.AESSalt))
	} else {
		fmt.Printf("  Salt: (none)\n")
	}
	fmt.Printf("  IV: %x\n", fileInfo.AESIV)
	fmt.Printf("  KDF Iterations: %d\n", fileInfo.KDFIterations)

	// Derive the AES key from the password
	fmt.Printf("  Deriving key from password...\n")
	key, err := deriveAESKey(de.password, fileInfo)
	if err != nil {
		return fmt.Errorf("failed to derive AES key: %w", err)
	}

	// Calculate which volume contains the start of this file
	volumeIndex, volumeOffset := de.calculateVolumePosition(fileInfo.Offset)
	fmt.Printf("  Starting in volume %d at offset %d\n", volumeIndex+1, volumeOffset)

	// Create a multi-volume reader starting at the file offset
	encryptedReader, err := newMultiVolumeReader(de.volumes, de.volumeSizes, fileInfo.Offset)
	if err != nil {
		return fmt.Errorf("failed to create volume reader: %w", err)
	}
	defer encryptedReader.Close()

	// Calculate packed size (encrypted data size)
	packedSize := fileInfo.PackedSize
	if packedSize == 0 {
		// If packed size is not available, estimate based on uncompressed size
		// AES-CBC adds padding, so packed size is at least ceil(size/16)*16
		packedSize = (fileInfo.Size + 15) / 16 * 16
	}

	// Limit the reader to only read the packed size
	limitedReader := io.LimitReader(encryptedReader, int64(packedSize))

	// Create streaming AES decoder
	fmt.Printf("  Creating streaming AES decoder...\n")
	aesReader, err := newAESDecoderReader(limitedReader, key, fileInfo.AESIV, fileInfo.Size)
	if err != nil {
		return fmt.Errorf("failed to create AES decoder: %w", err)
	}

	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Stream decrypt from archive to output file
	fmt.Printf("  Streaming decryption...\n")
	written, err := io.Copy(outFile, aesReader)
	if err != nil {
		return fmt.Errorf("failed to stream decrypt: %w", err)
	}

	if uint64(written) != fileInfo.Size {
		return fmt.Errorf("size mismatch: expected %d bytes, wrote %d bytes", fileInfo.Size, written)
	}

	fmt.Printf("  Successfully streamed and decrypted %d bytes to %s\n", written, outputPath)

	return nil
}

func main() {
	// Parse command-line arguments
	var (
		outputDir string
		maxFiles  int
		password  string
	)
	flag.StringVar(&outputDir, "o", "./extracted_files", "Output directory for extracted files")
	flag.IntVar(&maxFiles, "n", 3, "Maximum number of files to extract per type (0 for all)")
	flag.StringVar(&password, "p", "", "Password for encrypted archives")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <archive.7z or archive.7z.001>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Extract files from a 7zip archive using direct offset reading.\n")
		fmt.Fprintf(os.Stderr, "Supports both uncompressed and encrypted files.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s archive.7z\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -o ./output -n 5 multipart.7z.001\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -p mypassword -o ./output encrypted.7z\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nNote: Uncompressed files use direct offset reading.\n")
		fmt.Fprintf(os.Stderr, "      Encrypted files are decrypted using AES metadata from the archive.\n")
	}
	flag.Parse()

	// Check if archive path is provided
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	archivePath := flag.Arg(0)

	// Create the direct extractor
	extractor, err := NewDirectExtractor(archivePath, password)
	if err != nil {
		log.Fatalf("Failed to create extractor: %v", err)
	}
	defer extractor.Close()

	fmt.Printf("Archive opened: %s\n", filepath.Base(archivePath))
	if password != "" {
		fmt.Println("Using password for encrypted archives")
	}
	fmt.Printf("Number of volumes: %d\n\n", len(extractor.volumes))

	// Get file information with offsets
	// We use the sevenzip library ONLY to get metadata, not for extraction
	var reader *sevenzip.ReadCloser
	if password != "" {
		reader, err = sevenzip.OpenReaderWithPassword(archivePath, password)
	} else {
		reader, err = sevenzip.OpenReader(archivePath)
	}
	if err != nil {
		log.Fatalf("Failed to open archive for metadata: %v", err)
	}

	files, err := reader.ListFilesWithOffsets()
	if err != nil {
		reader.Close()
		log.Fatalf("Failed to list files: %v", err)
	}
	reader.Close() // Close immediately after getting metadata

	// Categorize files
	var uncompressedFiles []sevenzip.FileInfo
	var encryptedFiles []sevenzip.FileInfo
	var compressedFiles []sevenzip.FileInfo

	for _, file := range files {
		if file.Encrypted {
			encryptedFiles = append(encryptedFiles, file)
		} else if file.Compressed {
			compressedFiles = append(compressedFiles, file)
		} else {
			uncompressedFiles = append(uncompressedFiles, file)
		}
	}

	fmt.Printf("Found %d uncompressed files\n", len(uncompressedFiles))
	fmt.Printf("Found %d compressed files\n", len(compressedFiles))
	fmt.Printf("Found %d encrypted files\n", len(encryptedFiles))

	if len(uncompressedFiles) == 0 && len(encryptedFiles) == 0 {
		fmt.Println("\nNo uncompressed or encrypted files found in the archive")
		fmt.Println("Compressed files require standard extraction method")
		return
	}

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Extract uncompressed files
	if len(uncompressedFiles) > 0 {
		filesToExtract := maxFiles
		if maxFiles == 0 || maxFiles > len(uncompressedFiles) {
			filesToExtract = len(uncompressedFiles)
		}

		fmt.Printf("\n%s\n", string(make([]byte, 80)))
		fmt.Printf("Extracting %d uncompressed files using direct offset reading:\n", filesToExtract)
		fmt.Println(string(make([]byte, 80)))

		for i := 0; i < filesToExtract; i++ {
			file := uncompressedFiles[i]
			outputPath := filepath.Join(outputDir, fmt.Sprintf("direct_%s", filepath.Base(file.Name)))

			if err := extractor.ExtractFileByOffset(file, outputPath); err != nil {
				log.Printf("Failed to extract %s: %v", file.Name, err)
			}
		}
	}

	// Extract encrypted files if password is provided
	if len(encryptedFiles) > 0 && password != "" {
		filesToExtract := maxFiles
		if maxFiles == 0 || maxFiles > len(encryptedFiles) {
			filesToExtract = len(encryptedFiles)
		}

		fmt.Printf("\n%s\n", string(make([]byte, 80)))
		fmt.Printf("Extracting %d encrypted files using AES decryption:\n", filesToExtract)
		fmt.Println(string(make([]byte, 80)))

		for i := 0; i < filesToExtract; i++ {
			file := encryptedFiles[i]
			outputPath := filepath.Join(outputDir, fmt.Sprintf("encrypted_%s", filepath.Base(file.Name)))

			if err := extractor.ExtractEncryptedFileByOffset(file, outputPath); err != nil {
				log.Printf("Failed to extract encrypted file %s: %v", file.Name, err)
			}
		}
	} else if len(encryptedFiles) > 0 && password == "" {
		fmt.Printf("\n%d encrypted files found but no password provided (-p flag)\n", len(encryptedFiles))
		fmt.Println("Use -p <password> to extract encrypted files")
	}

	// Summary
	fmt.Printf("\n%s\n", string(make([]byte, 80)))
	fmt.Println("Extraction complete!")
	fmt.Printf("Files extracted to: %s\n", outputDir)

	// Information about the extraction methods
	fmt.Println("\nExtraction Methods Used:")
	if len(uncompressedFiles) > 0 {
		fmt.Println("\nDirect Offset Extraction (Uncompressed files):")
		fmt.Println("  ✓ No decompression overhead")
		fmt.Println("  ✓ Can seek directly to file offset")
		fmt.Println("  ✓ Faster for large uncompressed files")
		fmt.Println("  ✓ Lower memory usage")
	}
	if len(encryptedFiles) > 0 && password != "" {
		fmt.Println("\nAES Decryption (Encrypted files):")
		fmt.Println("  ✓ Uses AES metadata from archive (salt, IV, KDF iterations)")
		fmt.Println("  ✓ Direct decryption without full decompression pipeline")
		fmt.Println("  ✓ Works with stored+encrypted files")
		fmt.Println("  ℹ Compressed+encrypted files may need additional handling")
	}
	if len(compressedFiles) > 0 {
		fmt.Printf("\nNote: %d compressed files require standard extraction (File.Open() method)\n", len(compressedFiles))
	}
}
