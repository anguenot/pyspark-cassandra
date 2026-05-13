# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pyspark-cassandra is a Python port of the DataStax Spark Cassandra Connector. It provides PySpark RDD support for reading/writing Apache Cassandra CQL rows. The project is a hybrid Scala + Python codebase that produces a fat JAR bundled inside a pip-installable Python package.

## Build Commands

```bash
# Build the assembly JAR and copy it into the Python package
make dist
# Output: python/pyspark_cassandra/jars/pyspark-cassandra-assembly-<VERSION>.jar

# Compile Scala only
sbt compile

# Lint everything (Python flake8 via tox + Scala scalastyle)
make lint

# Python lint only
tox -e flake8
# or: flake8 --ignore=F401 python/pyspark_cassandra

# Scala style only
sbt -batch scalastyle
```

## Testing

Integration tests require a local Cassandra instance (managed via CCM). There are no unit tests — all tests hit a real Cassandra.

```bash
# Full cycle: build, start Cassandra, test, stop
make dist start-cassandra test-integration stop-cassandra

# Setup steps individually
make install-venv
make start-cassandra

# Run integration tests (requires JAR built and Cassandra running)
make test-integration-spark-3.5.5

# Run tests directly via spark-submit
source venv/bin/activate
lib/spark-3.5.5-bin-hadoop3/bin/spark-submit \
    --master local[*] \
    --driver-memory 512m \
    --conf spark.cassandra.connection.host="localhost" \
    --jars $(python -c "import pyspark_cassandra; print(pyspark_cassandra.jar_path())") \
    python/tests.py

# Teardown
make stop-cassandra
```

Test file: `python/tests.py` (unittest-based, all in one file).

## Architecture

### Python layer (`python/pyspark_cassandra/`)

- `__init__.py` — Monkey-patches `pyspark.rdd.RDD` with `saveToCassandra`, `joinWithCassandraTable`, `deleteFromCassandra` on import. Auto-adds bundled JAR to active SparkContext.
- `context.py` — `CassandraSparkContext` extending SparkContext with `cassandraTable()`
- `rdd.py` — `CassandraRDD` with Cassandra-specific operations (select, where, limit, spanBy, joinWithCassandraTable)
- `conf.py` — `ReadConf` / `WriteConf` / `ConnectionConf` for read/write/connection parameters
- `types.py` — `Row` and `UDT` types for CQL row mapping
- `format.py` — `RowFormat` enum (DICT, TUPLE, ROW) and `ColumnSelector`
- `util.py` — Py4J helper utilities (Java array/object conversion, PythonHelper loading)
- `jars/` — Contains the bundled assembly JAR (build artifact, gitignored)

### Scala bridge (`src/main/scala/`)

- `pyspark_cassandra/PythonHelper.scala` — Main JVM-side entry point called from Python via Py4J
- `pyspark_cassandra/Pickling.scala` — Python-JVM serialization via Pyrolite
- `pyspark_cassandra/RowReaders.scala` — Reads CQL rows into Python-compatible formats
- `pyspark_cassandra/RowWriter.scala` — Writes Python data to Cassandra
- `pyspark_cassandra/RowTransformers.scala` — Data type transformations
- `pyspark_util/` — General PySpark-JVM conversion utilities (pickling, conversions)

### Data flow

Python code → Py4J gateway → Scala PythonHelper → spark-cassandra-connector → Cassandra. Data is serialized between Python and JVM via Pyrolite pickling.

## Compatibility

| Component | Version |
|-----------|---------|
| Spark | 3.5.x |
| Scala | 2.12.18 |
| Java | 17 |
| spark-cassandra-connector | 3.5.1 |
| Pyrolite | 5.1 |
| Cassandra | 3.11.x, 4.0.x, 4.1.x |
| Python | 3.9+ |

## Distribution

The package is distributed via PyPI as a pip-installable wheel that bundles the assembly JAR. No `--packages` or separate JAR download needed. Build system is defined in `python/pyproject.toml`.

```bash
pip install pyspark-cassandra
```

## Release

Version is sourced from `version.txt` (used by both SBT and Python). Bump with:

```bash
bumpversion $(cat version.txt) --new-version=X.Y.Z
git push && git push --tags origin
make release-prod     # PyPI
```

## Key Files

- `version.txt` — Single source of truth for version (read by both SBT and setup.py)
- `python/pyproject.toml` — PEP 517 build system declaration
- `python/setup.py` — Package metadata and configuration
- `python/MANIFEST.in` — Ensures JAR is included in sdist
- `CHANGES.rst` — Changelog
