# External Merge Sort CSV Sort — POC

Sorts large CSV files using [externalsortinginjava](https://github.com/lemire/externalsortinginjava),
a lightweight Java library (~30 KB) that implements external merge sort.

## How It Works

1. **Split** — Reads the input file in chunks that fit in memory, sorts each chunk in-place
2. **Write** — Writes each sorted chunk to a temp file on disk
3. **Merge** — Merges all sorted temp files back into one sorted output (k-way merge)

Only one chunk is in memory at a time, so heap usage stays low regardless of file size.

## Build

```bash
cd poc/external-sort
mvn clean package -q
```

Produces a ~3 MB shaded JAR (vs ~200 MB for Spark).

## Generate Test Data

```bash
# 500 MB (5M rows x 5 cols) — identical data to Spark/Reactor POCs (same seed)
java -jar target/external-sort-csv-1.0-SNAPSHOT.jar --generate test_500mb.csv 5000000 5

# 2 GB (20M rows x 5 cols)
java -jar target/external-sort-csv-1.0-SNAPSHOT.jar --generate test_2gb.csv 20000000 5

# 6 GB (60M rows x 5 cols)
java -jar target/external-sort-csv-1.0-SNAPSHOT.jar --generate test_6gb.csv 60000000 5
```

## Run Sort

```bash
# Sort 500 MB file by column 1 with only 256 MB heap
java -Xmx256m -jar target/external-sort-csv-1.0-SNAPSHOT.jar test_500mb.csv sorted.csv 1

# Sort 2 GB file with 512 MB heap
java -Xmx512m -jar target/external-sort-csv-1.0-SNAPSHOT.jar test_2gb.csv sorted.csv 1

# Sort 6 GB file with 512 MB heap
java -Xmx512m -jar target/external-sort-csv-1.0-SNAPSHOT.jar test_6gb.csv sorted.csv 1
```

No `--add-opens` flags needed (unlike Spark). No heavy runtime startup.

## Expected Results

| File Size | Heap Needed | Ratio   | Why                                           |
|-----------|-------------|---------|-----------------------------------------------|
| 500 MB    | 100-256 MB  | 0.2-0.5x| Only one sorted chunk in memory at a time     |
| 2 GB      | 256-512 MB  | 0.1-0.3x| Same — chunk size stays bounded               |
| 6 GB      | 256-512 MB  | 0.04-0.1x| More temp files, same per-chunk memory        |

## Comparison: External Sort vs Spark vs Reactor

| Aspect               | External Sort                        | Spark                                  | Reactor                       |
|-----------------------|--------------------------------------|----------------------------------------|-------------------------------|
| Sort mechanism        | External merge sort (disk-based)     | Tungsten + ExternalSorter              | `collectSortedList()` in-heap |
| Memory model          | One chunk in JVM heap at a time      | Off-heap binary (UnsafeRow) + spill    | All data in JVM heap          |
| Heap needed (500 MB)  | ~100-256 MB (0.2-0.5x)              | ~300-500 MB (0.6-1.0x)                | ~2-3 GB (4-6x)               |
| Heap needed (6 GB)    | ~256-512 MB (0.04-0.1x)             | ~1-2 GB (0.2-0.3x)                    | ~24-36 GB (4-6x)             |
| JAR size              | ~3 MB                                | ~200 MB                                | ~5 MB                         |
| Startup time          | Instant                              | 5-10s (Spark init)                     | Instant                       |
| Java 17 flags         | None needed                          | `--add-opens` required                 | None needed                   |
| Distributed capable   | No (single JVM)                      | Yes (cluster mode)                     | No (single JVM)               |
| Library complexity    | Minimal (~30 KB dependency)          | Heavy (~200 MB, Scala runtime)         | Medium (~5 MB)                |

## When to Use What

- **External Sort**: Best for single-machine CSV sorting where simplicity and low memory matter.
  Ideal for Pentaho migration where jobs sort large files on a single server.
- **Spark**: Best when you need distributed processing across a cluster, or when doing
  complex transformations (joins, aggregations) beyond simple sorting.
- **Reactor**: Not suitable for large-file sorting (no disk spill). Good for I/O-bound
  concurrent workloads with backpressure.
