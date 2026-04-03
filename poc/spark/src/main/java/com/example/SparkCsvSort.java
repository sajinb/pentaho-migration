package com.example;

import com.opencsv.CSVWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import static org.apache.spark.sql.functions.col;

/**
 * Apache Spark CSV Sort — Memory Test
 *
 * Reads a CSV file using Spark DataFrame API, performs a full-dataset sort
 * (orderBy), and writes the sorted output.
 *
 * This demonstrates Spark's advantage: Tungsten off-heap binary format +
 * ExternalSorter disk spill keeps JVM heap usage low even for large files.
 *
 * Usage:
 *   java -Xmx512m --add-opens=java.base/java.lang=ALL-UNNAMED \
 *        --add-opens=java.base/java.nio=ALL-UNNAMED \
 *        --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
 *        --add-opens=java.base/java.util=ALL-UNNAMED \
 *        --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
 *        -jar spark-csv-sort-1.0-SNAPSHOT.jar input.csv output.csv sort_column_index
 *
 * Compare with ReactorCsvSort which needs 4-6x file size in JVM heap.
 */
public class SparkCsvSort {

    private static final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private static long peakHeapUsed = 0;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: java -Xmx<size> -jar spark-csv-sort.jar <input.csv> <output.csv> <sort_column_index>");
            System.out.println();
            System.out.println("Examples:");
            System.out.println("  java -Xmx512m -jar spark-csv-sort.jar data.csv sorted.csv 0");
            System.out.println("  java -Xmx1g  -jar spark-csv-sort.jar data.csv sorted.csv 2");
            System.out.println();
            System.out.println("To generate test data, use: --generate <output.csv> <rows> <columns>");
            System.out.println("  java -jar spark-csv-sort.jar --generate test.csv 10000000 5");
            System.out.println();
            System.out.println("Note: Java 17 requires --add-opens flags. See README for details.");
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
        System.out.println("=== Apache Spark CSV Sort — Memory Test ===");
        System.out.println("Input file:    " + inputPath);
        System.out.println("File size:     " + formatBytes(fileSize));
        System.out.println("Sort column:   " + sortColumnIndex);
        System.out.println("Max heap (-Xmx): " + formatBytes(Runtime.getRuntime().maxMemory()));
        System.out.println();

        // Start memory monitoring thread
        Thread memMonitor = startMemoryMonitor();

        Instant start = Instant.now();

        try {
            sortCsvWithSpark(inputPath, outputPath, sortColumnIndex);
        } catch (OutOfMemoryError e) {
            System.out.println();
            System.out.println("*** OUT OF MEMORY ***");
            System.out.println("Peak heap used: " + formatBytes(peakHeapUsed));
            System.out.println("Max heap available: " + formatBytes(Runtime.getRuntime().maxMemory()));
            System.out.println();
            System.out.println("Even Spark ran out of heap. Try increasing -Xmx slightly");
            System.out.println("or ensure spark.local.dir has enough disk space for spill.");
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
        System.out.println("Key insight: Spark used " + String.format("%.1fx", (double) peakHeapUsed / fileSize)
                + " the file size in JVM heap.");
        System.out.println("Reactor's collectSortedList() typically needs 4-6x (all in JVM heap, no spill).");
    }

    private static void sortCsvWithSpark(String inputPath, String outputPath, int sortColumnIndex) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkCsvSort")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "200")
                .config("spark.local.dir", System.getProperty("java.io.tmpdir") + "/spark-temp")
                .config("spark.driver.maxResultSize", "0")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        try {
            // Read CSV — all columns as strings for fair comparison with Reactor
            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "false")
                    .csv(inputPath);

            String[] columns = df.columns();
            String sortColumn = columns[sortColumnIndex];
            System.out.println("Header: " + Arrays.toString(columns));
            System.out.println("Sorting by column: " + sortColumn);

            long rowCount = df.count();
            updatePeakMemory();
            System.out.printf("Row count: %,d%n", rowCount);

            // Sort — Spark uses Tungsten off-heap + ExternalSorter with disk spill
            Dataset<Row> sorted = df.orderBy(col(sortColumn));

            // Write as single CSV file
            String tempOutputDir = outputPath + "_spark_temp";
            sorted.coalesce(1)
                    .write()
                    .option("header", "true")
                    .mode("overwrite")
                    .csv(tempOutputDir);

            updatePeakMemory();

            // Move single part file to desired output path
            try {
                moveSinglePartFile(tempOutputDir, outputPath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to move Spark output file", e);
            }
            System.out.printf("Wrote sorted output to %s%n", outputPath);
        } finally {
            spark.stop();
        }
    }

    /**
     * Spark writes to a directory with part-*.csv files. Extract the single
     * part file and clean up the temp directory.
     */
    private static void moveSinglePartFile(String sparkOutputDir, String targetPath) throws IOException {
        Path dir = Path.of(sparkOutputDir);
        Path target = Path.of(targetPath);

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "part-*.csv")) {
            for (Path partFile : stream) {
                Files.move(partFile, target, StandardCopyOption.REPLACE_EXISTING);
                break; // only one file with coalesce(1)
            }
        }

        // Clean up temp directory
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path file : stream) {
                Files.deleteIfExists(file);
            }
        }
        Files.deleteIfExists(dir);
    }

    /**
     * Generate a test CSV file with random data.
     * Identical to ReactorCsvSort.generateTestCsv for byte-identical test data.
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
