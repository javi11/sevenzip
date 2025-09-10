package main

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/bodgit/sevenzip"
)

func main() {
	// Path to your multipart 7zip file
	archivePath := "/Users/javi/SnelNL/downloads/Jurassic.World.2015.2160p.UHD.BluRay.REMUX.DTS-X.7.1.HDR.HEVC-UnKn0wn.mkv (1)/3i1odSJ622ygK5RuJMvULyGwZEFZQkKx.7z.001"

	fmt.Printf("Opening multipart archive: %s\n", filepath.Base(archivePath))
	fmt.Println("=" + string(make([]byte, 80)) + "=")

	// Open the multipart 7zip archive
	// Since the file ends with .001, it will automatically open all parts (.001, .002, etc.)
	reader, err := sevenzip.OpenReader(archivePath)
	if err != nil {
		log.Fatalf("Failed to open archive: %v", err)
	}
	defer reader.Close()

	// List all volumes that were opened
	volumes := reader.Volumes()
	fmt.Printf("Archive consists of %d volume(s):\n", len(volumes))
	for i, vol := range volumes {
		fmt.Printf("  Volume %d: %s\n", i+1, filepath.Base(vol))
	}
	fmt.Println()

	// Get file information with offsets
	files, err := reader.ListFilesWithOffsets()
	if err != nil {
		log.Fatalf("Failed to list files: %v", err)
	}

	fmt.Printf("Total files in archive: %d\n\n", len(files))

	// Separate files by type
	var uncompressedFiles []sevenzip.FileInfo
	var compressedFiles []sevenzip.FileInfo
	var encryptedFiles []sevenzip.FileInfo

	for _, file := range files {
		if file.Encrypted {
			encryptedFiles = append(encryptedFiles, file)
		} else if file.Compressed {
			compressedFiles = append(compressedFiles, file)
		} else {
			uncompressedFiles = append(uncompressedFiles, file)
		}
	}

	// Display uncompressed, non-encrypted files (these can be accessed directly at their offsets)
	if len(uncompressedFiles) > 0 {
		fmt.Println("UNCOMPRESSED FILES (Direct access possible):")
		fmt.Println("-" + string(make([]byte, 45)) + "-")
		fmt.Printf("%-50s %15s %15s\n", "Filename", "Offset", "Size")
		fmt.Println("-" + string(make([]byte, 82)) + "-")

		var totalSize uint64
		for _, file := range uncompressedFiles {
			fmt.Printf("%-50s %15d %15d\n",
				truncateString(file.Name, 50),
				file.Offset,
				file.Size)
			totalSize += file.Size
		}
		fmt.Printf("\nTotal size of uncompressed files: %d bytes (%.2f GB)\n\n",
			totalSize, float64(totalSize)/(1024*1024*1024))
	}

	// Display compressed files
	if len(compressedFiles) > 0 {
		fmt.Println("COMPRESSED FILES (Decompression required):")
		fmt.Println("-" + string(make([]byte, 43)) + "-")
		fmt.Printf("%-50s %15s %10s\n", "Filename", "Size", "Folder")
		fmt.Println("-" + string(make([]byte, 77)) + "-")

		for _, file := range compressedFiles {
			fmt.Printf("%-50s %15d %10d\n",
				truncateString(file.Name, 50),
				file.Size,
				file.FolderIndex)
		}
		fmt.Printf("\nTotal compressed files: %d\n\n", len(compressedFiles))
	}

	// Display encrypted files
	if len(encryptedFiles) > 0 {
		fmt.Println("ENCRYPTED FILES (Decryption required):")
		fmt.Println("-" + string(make([]byte, 39)) + "-")
		fmt.Printf("%-50s %15s %10s\n", "Filename", "Size", "Folder")
		fmt.Println("-" + string(make([]byte, 77)) + "-")

		for _, file := range encryptedFiles {
			fmt.Printf("%-50s %15d %10d\n",
				truncateString(file.Name, 50),
				file.Size,
				file.FolderIndex)
		}
		fmt.Printf("\nTotal encrypted files: %d\n", len(encryptedFiles))
	}

	// Summary
	fmt.Println("\n" + "=" + string(make([]byte, 80)) + "=")
	fmt.Println("SUMMARY:")
	fmt.Printf("  Total files:        %d\n", len(files))
	fmt.Printf("  Uncompressed files: %d (direct access possible)\n", len(uncompressedFiles))
	fmt.Printf("  Compressed files:   %d\n", len(compressedFiles))
	fmt.Printf("  Encrypted files:    %d\n", len(encryptedFiles))

	// Note about direct access
	if len(uncompressedFiles) > 0 {
		fmt.Println("\nNOTE: Uncompressed, non-encrypted files can be read directly from the archive")
		fmt.Println("      at their specified offsets without decompression overhead.")
	}
}

// Helper function to truncate long strings
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
