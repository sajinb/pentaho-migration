# Apache Spark CSV Sort — Memory Test

Tests JVM heap consumption when using Spark's Tungsten engine to sort a full CSV dataset.
Contrast with the [Reactor POC](../spring-reactor/) which needs 4-6x file size in JVM heap.

## Build

```bash
cd poc/spark
mvn clean package -q
```

> First build downloads ~100 MB of dependencies. The shaded JAR is ~200 MB.

## Generate Test Data

```bash
# ~500 MB CSV (5 million rows x 5 columns)
java -jar target/spark-csv-sort-1.0-SNAPSHOT.jar --generate test_500mb.csv 5000000 5

# ~2 GB CSV (20 million rows x 5 columns)
java -jar target/spark-csv-sort-1.0-SNAPSHOT.jar --generate test_2gb.csv 20000000 5

# ~6 GB CSV (60 million rows x 5 columns)
java -jar target/spark-csv-sort-1.0-SNAPSHOT.jar --generate test_6gb.csv 60000000 5
```

## Run Sort

Java 17 requires `--add-opens` flags for Spark's unsafe memory access:

```bash
JAVA_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED"

# 500 MB file with 512 MB heap
java -Xmx512m $JAVA_OPENS -jar target/spark-csv-sort-1.0-SNAPSHOT.jar test_500mb.csv sorted.csv 1

# 2 GB file with 1 GB heap
java -Xmx1g $JAVA_OPENS -jar target/spark-csv-sort-1.0-SNAPSHOT.jar test_2gb.csv sorted.csv 1

# 6 GB file with 2 GB heap
java -Xmx2g $JAVA_OPENS -jar target/spark-csv-sort-1.0-SNAPSHOT.jar test_6gb.csv sorted.csv 1
```

## What to Observe

The program prints:
- **Peak heap used** — maximum JVM heap consumed during the sort
- **Memory/file ratio** — how much heap was needed relative to file size
- **Duration** — wall-clock time including Spark initialization

### Expected Results (Spark)

| File Size | Heap Needed | Ratio | Why |
|---|---|---|---|
| 500 MB | ~300-500 MB | 0.6-1.0x | Tungsten binary format + disk spill |
| 2 GB | ~500 MB-1 GB | 0.3-0.5x | Sort spills to disk via ExternalSorter |
| 6 GB | ~1-2 GB | 0.2-0.3x | Most data stays off-heap or on disk |

### Comparison: Reactor vs Spark

| | Reactor | Spark |
|---|---|---|
| **Sort mechanism** | `Flux.collectSortedList()` | `DataFrame.orderBy()` + Tungsten |
| **Memory model** | All rows in JVM heap as `String[]` | Off-heap binary `UnsafeRow` format |
| **Disk spill** | None | ExternalSorter spills to `spark.local.dir` |
| **500 MB file heap** | ~2-3 GB (4-6x) | ~300-500 MB (0.6-1.0x) |
| **6 GB file heap** | ~24-36 GB (OOM on most servers) | ~1-2 GB |
| **Startup overhead** | ~1s | ~5-10s (Spark init) |

### How Spark Keeps Heap Low

1. **Tungsten binary format** — rows are stored as compact `UnsafeRow` byte arrays in off-heap memory, not as Java objects with per-field overhead
2. **Binary comparison** — sort comparisons operate on serialized bytes without deserializing to Java objects
3. **ExternalSorter disk spill** — when memory pressure is high, sorted runs are spilled to disk under `spark.local.dir` and merge-sorted on read-back

## Important Notes

- **`-Xmx` controls heap in fat-JAR mode**, not `spark.driver.memory` (that config only takes effect via `spark-submit`)
- **First run is slower** (~5-10s) due to Spark initialization overhead
- **Output handling**: Spark writes to directories; the code automatically extracts the single CSV file
- **`coalesce(1)`** forces a single output file — fine for POC, use multiple partitions in production
- Set `SPARK_LOCAL_DIRS` env var to a fast SSD for best spill performance
