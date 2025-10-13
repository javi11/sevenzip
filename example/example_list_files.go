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

func main() {
	// Parse command-line arguments
	var outputDir string
	flag.StringVar(&outputDir, "o", "./extracted_files", "Output directory for extracted files")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <archive.7z or archive.7z.001>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "List and extract files from a 7zip archive with offset information.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s archive.7z\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -o ./output multipart.7z.001\n", os.Args[0])
	}
	flag.Parse()

	// Check if archive path is provided
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	archivePath := flag.Arg(0)

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
		
		// Example: Extract the first uncompressed file
		fmt.Println("\n" + "=" + string(make([]byte, 80)) + "=")
		fmt.Println("EXTRACTION EXAMPLE:")
		
		if len(uncompressedFiles) > 0 {
			// Take the first uncompressed file as an example
			fileToExtract := uncompressedFiles[0]
			fmt.Printf("\nExtracting file: %s\n", fileToExtract.Name)
			fmt.Printf("  Offset: %d bytes\n", fileToExtract.Offset)
			fmt.Printf("  Size: %d bytes\n", fileToExtract.Size)
			
			// Method 1: Using the standard Open() method (works for all files)
			extractUsingStandardMethod(reader, fileToExtract, outputDir)
			
			// Method 2: Direct offset reading (only for uncompressed files)
			// This would require opening the archive file directly and seeking to the offset
			// Note: This is more complex with multipart archives as you need to handle volume boundaries
			fmt.Println("\nDirect offset extraction is possible for uncompressed files but requires:")
			fmt.Println("  1. Opening the archive file(s) directly")
			fmt.Println("  2. Calculating which volume contains the offset")
			fmt.Println("  3. Seeking to the correct position")
			fmt.Println("  4. Reading the exact number of bytes")
		}
	}
}

// extractUsingStandardMethod extracts a file using the standard 7zip API
func extractUsingStandardMethod(reader *sevenzip.ReadCloser, fileInfo sevenzip.FileInfo, outputDir string) {
	// Find the file in the archive
	for _, file := range reader.File {
		if file.Name == fileInfo.Name {
			// Open the file from the archive
			rc, err := file.Open()
			if err != nil {
				log.Printf("Failed to open file %s: %v", fileInfo.Name, err)
				return
			}
			defer rc.Close()
			
			// Create output directory if it doesn't exist
			if err := os.MkdirAll(outputDir, 0755); err != nil {
				log.Printf("Failed to create output directory: %v", err)
				return
			}
			
			// Create the output file
			outputPath := filepath.Join(outputDir, filepath.Base(fileInfo.Name))
			outFile, err := os.Create(outputPath)
			if err != nil {
				log.Printf("Failed to create output file: %v", err)
				return
			}
			defer outFile.Close()
			
			// Copy the file contents
			written, err := io.Copy(outFile, rc)
			if err != nil {
				log.Printf("Failed to extract file: %v", err)
				return
			}
			
			fmt.Printf("\nSuccessfully extracted using standard method:\n")
			fmt.Printf("  Output: %s\n", outputPath)
			fmt.Printf("  Bytes written: %d\n", written)
			
			return
		}
	}
	
	log.Printf("File %s not found in archive", fileInfo.Name)
}

// Helper function to truncate long strings
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
