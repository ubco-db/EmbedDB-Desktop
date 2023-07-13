# SQLite Vs SBITS Benchmarking

## Description

Here we compare this library (SBITS) with SQLite, the leader in small database technology. Since SQLite cannot run on nearly as small of a system as SBITS, these tests were performed on a Raspberry Pi in order to meet the requirements of both systems. Inserts as well as a variety of queries were benchmarked to compare the performance of each.

## Inserts

The inserts were performed one at a time using the uwa500K dataset, which describes a lot of different metrics, but we used air temperature, air pressure, and air speed.

## Queries

### Regular Queries

#### SQLite Queries

These queries used the `sqlite3_prepare` method, followed by binding any values required, and then running through the query using the `sqlite3_step` method. Then, the queries are finalized using the `sqlite3_finalize` method.

All the queries were performed on three different types of tables, excluding the select by data value. These tables were as follows:

1. key INTEGER PRIMARY KEY, value TEXT
2. key INTEGER PRIMARY KEY, value BLOB
3. key INTEGER PRIMARY KEY, airTemp INTEGER, airPressure INTEGER , windSpeed INTEGER

Note: for the select by data value queries, only the third table definition was used due to the complextity of efficiently extracting the data from the BLOB and TEXT fields, as those fields stored three seperate values stored as once piece of data

##### Options

There were several tuning options used for the SQLite Benchmark:

1. LOG_TYPE: The type of recovery logging done by the database. This can be a journal based system on a file (the default), an in memory version of the journal, and write-ahead logging (WAL).

2. SYNC: Adjusts the level of disk synchronisation SQLite performs. This can be set to full (the default), normal, and off. Higher levels mean the data is synchronised more often, but results in much slower insertion speed.

3. INDEX: Adds an index to the table based on the first data field.

For the [SQLite benchmarking data](results/sqlite-pi-results.txt), the settings used were WAL logging, SYNC OFF, and an index was created for the data.

#### SBITS Queries

In SBITS we used the `sbitsIterator` to set query bounds.

#### Queries Benchmarked

* SELECT \* FROM table
* SELECT \* FROM table WHERE key >= 974100996
  * Selects ~10% by timestamp (key)
* SELECT \* FROM table WHERE key >= 949756644
  * Selects ~90% by timestamp (key)
* SELECT \* FROM table WHERE data >= 700
  * Selects ~5% by air temperature
* SELECT \* FROM table WHERE data >= 420
  * Selects ~88% by air temperature
* SELECT \* FROM r WHERE key >= 958885776 AND data >= 450 AND data <= 650
  * Selects ~40% using both key and data restraints

### Key-Value Lookup

We also performed key-value lookups that would query for exactly one record. The test was performed by reading a list of known keys from a file and querying the database for it. We did this test in two different configurations: sequential and random. The random would be more representative of a real use case because if you need sequential lookup of keys, then an iterator would be preferred.

### Running the Benchmarks

To run the benchmarks performed, please use the included [makefile](scripts/makefile), and add the `Sqlite3.c` and `Sqlite3.h` files to the from the [SQLite Amalgamation Files](https://www.sqlite.org/2023/sqlite-amalgamation-3420000.zip) website `scripts` directory.
