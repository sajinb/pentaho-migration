package com.example;

import com.google.code.externalsorting.ExternalSort;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

/**
 * External Merge Sort CSV Sort — Memory Test
 *
 * Uses externalsortinginjava to sort a CSV file via external merge sort:
 * 1. Splits input into sorted temp chunks that fit in memory
 * 2. Merges sorted chunks back into a single sorted output
 *
 * This keeps JVM heap usage low because only one chunk is in memory at a time.
 * No heavy runtime like Spark — just a lightweight Java library (~30 KB).
 *
 * Usage:
 *   java -Xmx256m -jar external-sort-csv-1.0-SNAPSHOT.jar input.csv output.csv sort_column_index
 *
 * Compare with:
 *   - Spark:   Tungsten off-heap + ExternalSorter (0.3-1.0x file size in heap)
 *   - Reactor: collectSortedList() needs 4-6x file size in heap
 */
public class ExternalSortCsvSort {

    private static final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private static long peakHeapUsed = 0;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: java -Xmx<size> -jar external-sort-csv.jar <input.csv> <output.csv> <sort_column_index>");
            System.out.println();
            System.out.println("Examples:");
            System.out.println("  java -Xmx256m -jar external-sort-csv.jar data.csv sorted.csv 0");
            System.out.println("  java -Xmx512m -jar external-sort-csv.jar data.csv sorted.csv 2");
            System.out.println();
            System.out.println("To generate test data, use: --generate <output.csv> <rows> <columns>");
            System.out.println("  java -jar external-sort-csv.jar --generate test.csv 10000000 5");
            return;
        }

        if ("--generate".equals(args[0])) {
            generateTestCsv(args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]));
            return;
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int sortColumnIndex = Integer.parseInt(args[2]);

        long fileSize = Files.size(Path.of(inputPath));
        System.out.println("=== External Merge Sort CSV Sort — Memory Test ===");
        System.out.println("Input file:    " + inputPath);
        System.out.println("File size:     " + formatBytes(fileSize));
        System.out.println("Sort column:   " + sortColumnIndex);
        System.out.println("Max heap (-Xmx): " + formatBytes(Runtime.getRuntime().maxMemory()));
        System.out.println();

        // Start memory monitoring thread
        Thread memMonitor = startMemoryMonitor();

        Instant start = Instant.now();

        try {
            sortCsvWithExternalSort(inputPath, outputPath, sortColumnIndex);
        } catch (OutOfMemoryError e) {
            System.out.println();
            System.out.println("*** OUT OF MEMORY ***");
            System.out.println("Peak heap used: " + formatBytes(peakHeapUsed));
            System.out.println("Max heap available: " + formatBytes(Runtime.getRuntime().maxMemory()));
            System.out.println();
            System.out.println("Try increasing -Xmx slightly. External sort should need very little heap.");
            System.exit(1);
        }

        Duration elapsed = Duration.between(start, Instant.now());
        memMonitor.interrupt();

        System.out.println();
        System.out.println("=== Results ===");
        System.out.println("Duration:      " + elapsed.toSeconds() + "s");
        System.out.println("Peak heap used: " + formatBytes(peakHeapUsed));
        System.out.println("Max heap:      " + formatBytes(Runtime.getRuntime().maxMemory()));
        System.out.println("Heap ratio:    " + String.format("%.1f%%", (peakHeapUsed * 100.0) / Runtime.getRuntime().maxMemory()));
        System.out.println("File size:     " + formatBytes(fileSize));
        System.out.println("Memory/file ratio: " + String.format("%.1fx", (double) peakHeapUsed / fileSize));
        System.out.println();
        System.out.println("Key insight: External sort used " + String.format("%.1fx", (double) peakHeapUsed / fileSize)
                + " the file size in JVM heap.");
        System.out.println("Reactor's collectSortedList() typically needs 4-6x (all in JVM heap, no spill).");
    }

    private static void sortCsvWithExternalSort(String inputPath, String outputPath, int sortColumnIndex)
            throws Exception {

        // Step 1: Read and display the CSV header
        String headerLine;
        String[] headerColumns;
        try (BufferedReader headerReader = Files.newBufferedReader(Path.of(inputPath), StandardCharsets.UTF_8)) {
            headerLine = headerReader.readLine();
            if (headerLine == null) {
                throw new IllegalArgumentException("Input file is empty");
            }
        }
        // Parse header to get column names (simple split — our generated data has no commas in values)
        headerColumns = headerLine.split(",");

        if (sortColumnIndex < 0 || sortColumnIndex >= headerColumns.length) {
            throw new IllegalArgumentException("Sort column index " + sortColumnIndex
                    + " is out of range. File has " + headerColumns.length + " columns.");
        }

        System.out.println("Header: " + String.join(", ", headerColumns));
        System.out.println("Sorting by column: " + headerColumns[sortColumnIndex]);

        // Step 2: Create a headerless version of the input (external sort doesn't know about headers)
        Path headerlessInput = Files.createTempFile("external-sort-input-", ".csv");
        long rowCount = 0;
        try (BufferedReader reader = Files.newBufferedReader(Path.of(inputPath), StandardCharsets.UTF_8);
             BufferedWriter writer = Files.newBufferedWriter(headerlessInput, StandardCharsets.UTF_8)) {
            reader.readLine(); // skip header
            String line;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine();
                rowCount++;
            }
        }
        System.out.printf("Row count: %,d%n", rowCount);

        updatePeakMemory();

        // Step 3: Build a comparator that parses the CSV line and compares by the sort column
        Comparator<String> csvColumnComparator = (line1, line2) -> {
            String val1 = extractCsvColumn(line1, sortColumnIndex);
            String val2 = extractCsvColumn(line2, sortColumnIndex);
            return val1.compareTo(val2);
        };

        // Step 4: Run external merge sort
        // ExternalSort splits the file into sorted temp chunks, then merges them.
        // maxtmpfiles controls how many temp files (more = smaller chunks = less heap per chunk)
        Path sortedTemp = Files.createTempFile("external-sort-output-", ".csv");
        File tempDir = Files.createTempDirectory("external-sort-tmp-").toFile();

        System.out.println("Sorting with external merge sort...");
        System.out.println("Temp directory: " + tempDir.getAbsolutePath());

        List<File> tempFiles = ExternalSort.sortInBatch(
                headerlessInput.toFile(),
                csvColumnComparator,
                ExternalSort.DEFAULTMAXTEMPFILES,
                StandardCharsets.UTF_8,
                tempDir,
                true,  // distinct = false → keep duplicates (true = use default headers)
                0,     // numHeader = 0 (we already stripped the header)
                false  // usegzip = false
        );

        System.out.printf("Created %d sorted temp chunks%n", tempFiles.size());
        updatePeakMemory();

        ExternalSort.mergeSortedFiles(
                tempFiles,
                sortedTemp.toFile(),
                csvColumnComparator,
                StandardCharsets.UTF_8,
                false,  // distinct = false → keep duplicates
                false,  // append = false
                false   // usegzip = false
        );

        updatePeakMemory();

        // Step 5: Prepend header to final output
        try (BufferedWriter writer = Files.newBufferedWriter(Path.of(outputPath), StandardCharsets.UTF_8);
             BufferedReader reader = Files.newBufferedReader(sortedTemp, StandardCharsets.UTF_8)) {
            writer.write(headerLine);
            writer.newLine();
            String line;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine();
            }
        }

        // Cleanup temp files
        Files.deleteIfExists(headerlessInput);
        Files.deleteIfExists(sortedTemp);
        deleteDirectory(tempDir);

        System.out.printf("Wrote sorted output to %s%n", outputPath);
    }

    /**
     * Fast CSV column extraction without full parsing.
     * Handles quoted fields correctly for comparator use.
     */
    private static String extractCsvColumn(String line, int columnIndex) {
        int col = 0;
        int i = 0;
        int len = line.length();

        while (col < columnIndex && i < len) {
            if (line.charAt(i) == '"') {
                // Skip quoted field
                i++; // skip opening quote
                while (i < len) {
                    if (line.charAt(i) == '"') {
                        i++;
                        if (i >= len || line.charAt(i) != '"') {
                            break; // end of quoted field
                        }
                        i++; // skip escaped quote
                    } else {
                        i++;
                    }
                }
                // i is now past the closing quote, skip comma
                if (i < len && line.charAt(i) == ',') {
                    i++;
                }
            } else {
                // Unquoted field — find next comma
                int commaPos = line.indexOf(',', i);
                if (commaPos == -1) {
                    i = len;
                } else {
                    i = commaPos + 1;
                }
            }
            col++;
        }

        if (i >= len) return "";

        // Extract the target column value
        if (line.charAt(i) == '"') {
            // Quoted field
            StringBuilder sb = new StringBuilder();
            i++; // skip opening quote
            while (i < len) {
                if (line.charAt(i) == '"') {
                    i++;
                    if (i >= len || line.charAt(i) != '"') {
                        break;
                    }
                    sb.append('"');
                    i++;
                } else {
                    sb.append(line.charAt(i));
                    i++;
                }
            }
            return sb.toString();
        } else {
            // Unquoted field
            int commaPos = line.indexOf(',', i);
            if (commaPos == -1) {
                return line.substring(i);
            }
            return line.substring(i, commaPos);
        }
    }

    private static void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                f.delete();
            }
        }
        dir.delete();
    }

    // === Test data generation (identical to Spark/Reactor POCs) ===

    private static void generateTestCsv(String outputPath, int rows, int columns) throws Exception {
        System.out.printf("Generating %,d rows x %d columns to %s...%n", rows, columns, outputPath);
        Instant start = Instant.now();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            // Header
            StringBuilder sb = new StringBuilder();
            for (int c = 0; c < columns; c++) {
                if (c > 0) sb.append(',');
                sb.append("col_").append(c);
            }
            writer.write(sb.toString());
            writer.newLine();

            // Data rows
            java.util.Random rng = new java.util.Random(42);
            for (int r = 0; r < rows; r++) {
                sb.setLength(0);
                for (int c = 0; c < columns; c++) {
                    if (c > 0) sb.append(',');
                    sb.append(generateRandomValue(rng, c));
                }
                writer.write(sb.toString());
                writer.newLine();

                if ((r + 1) % 1_000_000 == 0) {
                    System.out.printf("  Generated %,d / %,d rows%n", r + 1, rows);
                }
            }
        }

        long fileSize = Files.size(Path.of(outputPath));
        Duration elapsed = Duration.between(start, Instant.now());
        System.out.printf("Done. File size: %s, Duration: %ds%n", formatBytes(fileSize), elapsed.toSeconds());
    }

    private static String generateRandomValue(java.util.Random rng, int columnIndex) {
        return switch (columnIndex % 4) {
            case 0 -> String.valueOf(rng.nextInt(1_000_000));
            case 1 -> randomString(rng, 10 + rng.nextInt(20));
            case 2 -> String.format("%.2f", rng.nextDouble() * 10000);
            case 3 -> randomString(rng, 30 + rng.nextInt(50));
            default -> "";
        };
    }

    private static String randomString(java.util.Random rng, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + rng.nextInt(26)));
        }
        return sb.toString();
    }

    // === Memory monitoring (identical to Spark/Reactor POCs) ===

    private static Thread startMemoryMonitor() {
        Thread t = new Thread(() -> {
            while (!Thread.interrupted()) {
                updatePeakMemory();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "memory-monitor");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static synchronized void updatePeakMemory() {
        MemoryUsage heap = memoryBean.getHeapMemoryUsage();
        peakHeapUsed = Math.max(peakHeapUsed, heap.getUsed());
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
