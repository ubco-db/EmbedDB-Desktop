# SQLite Vs SBITS

## Description

Here we compare this library (SBITS) with SQLite, the leader in small database technology. Since SQLite cannot run on nearly as small of a system as SBITS, these tests were performed on a Raspberry Pi in order to meet the requirements of both systems. Inserts as well as a variety of queries were benchmarked to compare the performance of each.

## Inserts

The inserts were performed one at a time using the uwa500K dataset, which describes a lot of different metrics, but we used air temperature, air pressure, and air speed.

## Queries

### Regular queries

These queries used a regular SQL query for SQLite and was traversed through the result using `sqlite3_step`. In SBITS we used the `sbitsIterator` to set query bounds.

-   SELECT \* FROM table
-   SELECT \* FROM table WHERE key >= 974100996
    -   Selects ~10% by timestamp (key)
-   SELECT \* FROM table WHERE key >= 949756644
    -   Selects ~90% by timestamp (key)
-   SELECT \* FROM table WHERE data >= 700
    -   Selects ~5% by air temperature
-   SELECT \* FROM table WHERE data >= 420
    -   Selects ~88% by air temperature
-   SELECT \* FROM r WHERE key >= 958885776 AND data >= 450 AND data <= 650
    -   Selects ~40% using both key and data restraints

### Key-Value lookups

We also performed key-value lookups that would query for exactly one record. The test was performed by reading a list of known keys from a file and querying the database for it. We did this test in two different configurations: sequential and random. The random would be more representative of a real use case because if you need sequential lookup of keys, then an iterator would be preferred.
