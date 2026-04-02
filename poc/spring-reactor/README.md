# Spring Reactor CSV Sort — Memory Test

Tests JVM heap consumption when using Project Reactor to sort a full CSV dataset.

## Build

```bash
cd poc/spring-reactor
mvn clean package -q
```

## Generate Test Data

```bash
# Generate a ~500 MB CSV (5 million rows x 5 columns)
java -jar target/reactor-csv-sort-1.0-SNAPSHOT.jar --generate test_500mb.csv 5000000 5

# Generate a ~2 GB CSV (20 million rows x 5 columns)
java -jar target/reactor-csv-sort-1.0-SNAPSHOT.jar --generate test_2gb.csv 20000000 5

# Generate a ~6 GB CSV (60 million rows x 5 columns)
java -jar target/reactor-csv-sort-1.0-SNAPSHOT.jar --generate test_6gb.csv 60000000 5
```

## Run Sort with Different Heap Sizes

```bash
# Try with 4 GB heap — will likely OOM for files > 500 MB
java -Xmx4g -verbose:gc -jar target/reactor-csv-sort-1.0-SNAPSHOT.jar test_500mb.csv sorted.csv 1

# Try with 8 GB heap
java -Xmx8g -verbose:gc -jar target/reactor-csv-sort-1.0-SNAPSHOT.jar test_2gb.csv sorted.csv 1

# Try with 16 GB heap — will likely OOM for 6 GB file
java -Xmx16g -verbose:gc -jar target/reactor-csv-sort-1.0-SNAPSHOT.jar test_6gb.csv sorted.csv 1

# Try with 24 GB heap
java -Xmx24g -verbose:gc -jar target/reactor-csv-sort-1.0-SNAPSHOT.jar test_6gb.csv sorted.csv 1
```

## What to Observe

The program prints:
- **Peak heap used** — maximum JVM heap consumed during the sort
- **Memory/file ratio** — how much heap was needed relative to file size
- **Progress updates** every 1M rows with current heap usage

### Expected Results

| File Size | Heap Needed | Ratio | Why |
|---|---|---|---|
| 500 MB | ~2-3 GB | 4-6x | Each CSV row → Java String[] object + String objects per field |
| 2 GB | ~8-12 GB | 4-6x | Same ratio, larger scale |
| 6 GB | ~24-36 GB | 4-6x | Beyond 32 GB server capacity |

### Why the 4-6x Memory Expansion

Each CSV row on disk is plain text. When parsed into Java objects:
- Each `String[]` array: 16 bytes header + 8 bytes per reference
- Each `String` value: 40+ bytes overhead (object header + char[]/byte[] + length + hash)
- A 50-byte CSV row becomes ~200-300 bytes of Java objects
- Plus: `List<String[]>` backing array, sort algorithm temp space, GC overhead

### Key Takeaway

`Flux.collectSortedList()` is a **blocking operation** that loads the entire dataset into JVM heap.
There is no disk-spill mechanism in Reactor. For large-file sorts, use Spark (Tungsten off-heap + disk spill)
or DuckDB (columnar + disk spill).
```

## Compare with Spark (optional)

Create a similar test with PySpark:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.local.dir", "/tmp/spark-temp") \
    .getOrCreate()

df = spark.read.csv("test_6gb.csv", header=True, inferSchema=True)
df.orderBy("col_1").write.csv("sorted_spark", header=True)
```

Spark will handle the 6 GB file with only 4 GB driver memory by spilling to disk.
