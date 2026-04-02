package com.example;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.FileReader;
import java.io.FileWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Spring Reactor CSV Sort — Memory Test
 *
 * Reads a CSV file using Reactor Flux, performs a full-dataset sort
 * (collectSortedList), and writes the sorted output.
 *
 * This demonstrates the memory limitation: collectSortedList() must
 * load ALL rows into JVM heap before sorting.
 *
 * Usage:
 *   java -Xmx4g -jar reactor-csv-sort-1.0-SNAPSHOT.jar input.csv output.csv sort_column_index
 *
 * Try different -Xmx values to see when it OOMs:
 *   java -Xmx2g  -jar ...   # Small heap
 *   java -Xmx8g  -jar ...   # Medium heap
 *   java -Xmx16g -jar ...   # Large heap
 */
public class ReactorCsvSort {

    private static final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private static long peakHeapUsed = 0;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: java -Xmx<size> -jar reactor-csv-sort.jar <input.csv> <output.csv> <sort_column_index>");
            System.out.println();
            System.out.println("Examples:");
            System.out.println("  java -Xmx4g -jar reactor-csv-sort.jar data.csv sorted.csv 0");
            System.out.println("  java -Xmx8g -jar reactor-csv-sort.jar data.csv sorted.csv 2");
            System.out.println();
            System.out.println("To generate test data, use: --generate <output.csv> <rows> <columns>");
            System.out.println("  java -jar reactor-csv-sort.jar --generate test.csv 10000000 5");
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
        System.out.println("=== Spring Reactor CSV Sort — Memory Test ===");
        System.out.println("Input file:    " + inputPath);
        System.out.println("File size:     " + formatBytes(fileSize));
        System.out.println("Sort column:   " + sortColumnIndex);
        System.out.println("Max heap (-Xmx): " + formatBytes(Runtime.getRuntime().maxMemory()));
        System.out.println();

        // Start memory monitoring thread
        Thread memMonitor = startMemoryMonitor();

        Instant start = Instant.now();

        try {
            sortCsvWithReactor(inputPath, outputPath, sortColumnIndex);
        } catch (OutOfMemoryError e) {
            System.out.println();
            System.out.println("*** OUT OF MEMORY ***");
            System.out.println("Peak heap used: " + formatBytes(peakHeapUsed));
            System.out.println("Max heap available: " + formatBytes(Runtime.getRuntime().maxMemory()));
            System.out.println();
            System.out.println("This demonstrates the Reactor limitation: collectSortedList()");
            System.out.println("loads ALL rows into JVM heap with no disk spill option.");
            System.out.println("Try increasing -Xmx or use a smaller input file.");
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
        System.out.println("Key insight: Reactor needed " + String.format("%.1fx", (double) peakHeapUsed / fileSize)
                + " the file size in JVM heap.");
        System.out.println("Spark Tungsten would use ~0.7-1.3x in off-heap binary + disk spill.");
    }

    private static void sortCsvWithReactor(String inputPath, String outputPath, int sortColumnIndex) {
        AtomicLong rowCount = new AtomicLong(0);

        // Read CSV header
        String[] header;
        try (CSVReader reader = new CSVReader(new FileReader(inputPath))) {
            header = reader.readNext();
        } catch (Exception e) {
            throw new RuntimeException("Failed to read CSV header", e);
        }

        System.out.println("Header: " + Arrays.toString(header));
        System.out.println("Reading and sorting...");

        // ===== THIS IS THE KEY PART =====
        // Flux reads line-by-line (streaming), but collectSortedList() blocks
        // and loads ALL rows into a List<String[]> in JVM heap.
        // There is no way to avoid this for a full-dataset sort in Reactor.

        List<String[]> sorted = Flux.using(
                () -> {
                    CSVReader r = new CSVReader(new FileReader(inputPath));
                    r.readNext(); // skip header
                    return r;
                },
                reader -> Flux.fromIterable(() -> reader.iterator()),
                reader -> {
                    try { reader.close(); } catch (Exception e) { /* ignore */ }
                }
        )
        .doOnNext(row -> {
            long count = rowCount.incrementAndGet();
            if (count % 1_000_000 == 0) {
                updatePeakMemory();
                System.out.printf("  Read %,d rows — heap: %s / %s%n",
                        count,
                        formatBytes(memoryBean.getHeapMemoryUsage().getUsed()),
                        formatBytes(Runtime.getRuntime().maxMemory()));
            }
        })
        // ===== BLOCKING SORT — ALL ROWS IN MEMORY =====
        .collectSortedList(Comparator.comparing(
                (String[] row) -> sortColumnIndex < row.length ? row[sortColumnIndex] : "",
                Comparator.naturalOrder()
        ))
        .block(); // blocks until all rows collected and sorted

        updatePeakMemory();
        System.out.printf("Sorted %,d rows. Writing output...%n", rowCount.get());

        // Write sorted output
        try (CSVWriter writer = new CSVWriter(new FileWriter(outputPath))) {
            writer.writeNext(header);
            for (String[] row : sorted) {
                writer.writeNext(row);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write output", e);
        }

        System.out.printf("Wrote %,d rows to %s%n", rowCount.get(), outputPath);
    }

    /**
     * Generate a test CSV file with random data.
     */
    private static void generateTestCsv(String outputPath, int rows, int columns) throws Exception {
        System.out.printf("Generating %,d rows x %d columns to %s...%n", rows, columns, outputPath);
        Instant start = Instant.now();

        try (CSVWriter writer = new CSVWriter(new FileWriter(outputPath))) {
            // Header
            String[] header = new String[columns];
            for (int c = 0; c < columns; c++) {
                header[c] = "col_" + c;
            }
            writer.writeNext(header);

            // Data rows
            java.util.Random rng = new java.util.Random(42);
            String[] row = new String[columns];
            for (int r = 0; r < rows; r++) {
                for (int c = 0; c < columns; c++) {
                    row[c] = generateRandomValue(rng, c);
                }
                writer.writeNext(row);

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
            case 0 -> String.valueOf(rng.nextInt(1_000_000));                         // integer
            case 1 -> randomString(rng, 10 + rng.nextInt(20));                        // short string
            case 2 -> String.format("%.2f", rng.nextDouble() * 10000);                // decimal
            case 3 -> randomString(rng, 30 + rng.nextInt(50));                        // longer string
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
