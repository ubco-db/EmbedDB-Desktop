# Getting Started:

### Dependencies

1. GNU Make 4.4
2. PIP 23.0 to run testing suite.

## Running Benchmarks

To begin, build the project using the included [makefile](../makefile). There are  three files that run a benchmark of embedDB-Desktop

### EmbedDB Benchmark 

`embedDBBenchmark` is a benchmark of inserting and querying EmbedDB, and tests the inserted data for correctness.

```bash
make embedDBBenchmark
```

`embedDBVariableBenchmark` run a benchmark of inserting and querying EmbedDBwith variable length data and tests the inserted data for correctness.

```bash
make embedDBVariableBenchmark
```

`queryExample` file includes a benchmark of querying EmbedDB using the advanced query interface.


```bash
make queryExample
```

## Running the Tests

```bash
make test
```