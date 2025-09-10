package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"text/tabwriter"

	"github.com/javi11/sevenzip"
)

func main() {
	// Command line flags
	var (
		password = flag.String("p", "", "Password for encrypted archives")
		verbose  = flag.Bool("v", false, "Verbose output")
		help     = flag.Bool("h", false, "Show help")
	)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <archive.7z or archive.7z.001>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "List files in a 7zip archive with their offsets and compression status.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s archive.7z\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -p mypassword encrypted.7z\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s multipart.7z.001\n", os.Args[0])
	}

	flag.Parse()

	if *help || flag.NArg() < 1 {
		flag.Usage()
		os.Exit(0)
	}

	archivePath := flag.Arg(0)

	// Open the archive
	var reader *sevenzip.ReadCloser
	var err error

	if *password != "" {
		reader, err = sevenzip.OpenReaderWithPassword(archivePath, *password)
	} else {
		reader, err = sevenzip.OpenReader(archivePath)
	}

	if err != nil {
		log.Fatalf("Failed to open archive: %v", err)
	}
	defer reader.Close()

	// Show archive information
	fmt.Printf("Archive: %s\n", filepath.Base(archivePath))

	// List volumes if multipart
	volumes := reader.Volumes()
	if len(volumes) > 1 {
		fmt.Printf("Multipart archive with %d volumes:\n", len(volumes))
		if *verbose {
			for i, vol := range volumes {
				fmt.Printf("  [%d] %s\n", i+1, filepath.Base(vol))
			}
		}
	}
	fmt.Println()

	// Get file information with offsets
	files, err := reader.ListFilesWithOffsets()
	if err != nil {
		log.Fatalf("Failed to list files: %v", err)
	}

	// Create a tabwriter for better formatting
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	// Print header
	fmt.Fprintln(w, "Type\tOffset\tSize\tFolder\tName")
	fmt.Fprintln(w, "----\t------\t----\t------\t----")

	// Statistics
	var (
		uncompressedCount int
		compressedCount   int
		encryptedCount    int
		totalSize         uint64
	)

	// Print file information
	for _, file := range files {
		var fileType string

		if file.Encrypted {
			fileType = "ENC"
			encryptedCount++
		} else if file.Compressed {
			fileType = "COMP"
			compressedCount++
		} else {
			fileType = "STORE"
			uncompressedCount++
		}

		totalSize += file.Size

		// Format the output
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%s\n",
			fileType,
			file.Offset,
			file.Size,
			file.FolderIndex,
			file.Name)
	}

	w.Flush()
	fmt.Println()

	// Print summary
	fmt.Println("Summary:")
	fmt.Printf("  Total files:     %d\n", len(files))
	fmt.Printf("  Total size:      %d bytes (%.2f GB)\n", totalSize, float64(totalSize)/(1024*1024*1024))
	fmt.Printf("  Stored:          %d files (direct access possible)\n", uncompressedCount)
	fmt.Printf("  Compressed:      %d files\n", compressedCount)
	fmt.Printf("  Encrypted:       %d files\n", encryptedCount)

	if uncompressedCount > 0 {
		fmt.Println("\nNote: Files marked as 'STORE' can be read directly at their offsets without decompression.")
	}
}
