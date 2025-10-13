package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/javi11/sevenzip"
)

// DirectExtractor demonstrates direct extraction of uncompressed files from a 7zip archive
type DirectExtractor struct {
	reader      *sevenzip.ReadCloser
	volumes     []string
	volumeSizes []int64
}

// NewDirectExtractor creates a new direct extractor
func NewDirectExtractor(archivePath string) (*DirectExtractor, error) {
	reader, err := sevenzip.OpenReader(archivePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open archive: %w", err)
	}

	de := &DirectExtractor{
		reader:  reader,
		volumes: reader.Volumes(),
	}

	// Get the size of each volume for offset calculations
	de.volumeSizes = make([]int64, len(de.volumes))
	for i, vol := range de.volumes {
		info, err := os.Stat(vol)
		if err != nil {
			reader.Close()
			return nil, fmt.Errorf("failed to stat volume %s: %w", vol, err)
		}
		de.volumeSizes[i] = info.Size()
	}

	return de, nil
}

// Close closes the reader
func (de *DirectExtractor) Close() error {
	return de.reader.Close()
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

func main() {
	// Parse command-line arguments
	var (
		outputDir string
		maxFiles  int
	)
	flag.StringVar(&outputDir, "o", "./extracted_files", "Output directory for extracted files")
	flag.IntVar(&maxFiles, "n", 3, "Maximum number of files to extract (0 for all)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <archive.7z or archive.7z.001>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Extract uncompressed files from a 7zip archive using direct offset reading.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s archive.7z\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -o ./output -n 5 multipart.7z.001\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nNote: This tool only extracts uncompressed, non-encrypted files.\n")
	}
	flag.Parse()

	// Check if archive path is provided
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	archivePath := flag.Arg(0)

	// Create the direct extractor
	extractor, err := NewDirectExtractor(archivePath)
	if err != nil {
		log.Fatalf("Failed to create extractor: %v", err)
	}
	defer extractor.Close()

	fmt.Printf("Archive opened: %s\n", filepath.Base(archivePath))
	fmt.Printf("Number of volumes: %d\n\n", len(extractor.volumes))

	// Get file information with offsets
	files, err := extractor.reader.ListFilesWithOffsets()
	if err != nil {
		log.Fatalf("Failed to list files: %v", err)
	}

	// Find uncompressed files
	var uncompressedFiles []sevenzip.FileInfo
	for _, file := range files {
		if !file.Compressed && !file.Encrypted {
			uncompressedFiles = append(uncompressedFiles, file)
		}
	}

	fmt.Printf("Found %d uncompressed files that can be extracted directly\n", len(uncompressedFiles))

	if len(uncompressedFiles) == 0 {
		fmt.Println("No uncompressed files found in the archive")
		return
	}

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Determine how many files to extract
	filesToExtract := maxFiles
	if maxFiles == 0 || maxFiles > len(uncompressedFiles) {
		filesToExtract = len(uncompressedFiles)
	}

	fmt.Printf("\nExtracting %d uncompressed files using direct offset reading:\n", filesToExtract)
	fmt.Println("=" + string(make([]byte, 60)) + "=")

	for i := 0; i < filesToExtract; i++ {
		file := uncompressedFiles[i]
		outputPath := filepath.Join(outputDir, fmt.Sprintf("direct_%s", filepath.Base(file.Name)))

		if err := extractor.ExtractFileByOffset(file, outputPath); err != nil {
			log.Printf("Failed to extract %s: %v", file.Name, err)
		}
	}

	fmt.Println("\n" + "=" + string(make([]byte, 60)) + "=")
	fmt.Println("Direct extraction complete!")
	fmt.Printf("Files extracted to: %s\n", outputDir)

	// Compare with standard extraction method
	fmt.Println("\nComparison with standard extraction method:")
	fmt.Println("Direct extraction advantages:")
	fmt.Println("  - No decompression overhead")
	fmt.Println("  - Can seek directly to file offset")
	fmt.Println("  - Faster for large uncompressed files")
	fmt.Println("  - Lower memory usage")
	fmt.Println("\nLimitations:")
	fmt.Println("  - Only works for uncompressed, non-encrypted files")
	fmt.Println("  - Requires understanding of archive structure")
	fmt.Println("  - More complex with multipart archives")
}
