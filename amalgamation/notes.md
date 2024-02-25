I think the spline needs to go first because EmbedDB.h includes Spline first and then uses it to make a struct 

## Order of Dependencies: 


![image](/Blank%20diagram.png "Dependency Diagram")

- Spline -> RadixSpline -> EmbedDB.h 
- schema.h -> advancedQueries <- EmbedDB.h

## Benchmarks and Sizes 

For EmbedDBExample
- Query_type = 3
- Sequential Data

### Amaglamation

#### Output Size: 

Output of a single file w/example: 104kb 

#### Results: 

RESULTS: 
```bash
EmbedDB State Initialization Stats:
Buffer size: 4  Page size: 512
Key size: 4 Data size: 12 Record size: 16
Use index: 1  Max/min: 0 Sum: 0 Bmap: 1
Header size: 7  Records per page: 31


INSERT TEST:
Elapsed Time: 1 ms
Records inserted: 1000
Num reads: 0
Buffer hits: 0
Num writes: 33
Num index reads: 0
Num index writes: 1
Max Error: 31
Spline max error (0):
Spline points (2):
[0]: (0, 0)
[1]: (992, 0)



QUERY TEST:
Read records: 1000
Num: 1000 KEY: 26 Perc: 1000 Records: 1000 Reads: 33
Elapsed Time: 0 ms
Records queried: 1000
Num reads: 33
Buffer hits: 1000
Num writes: 0
Num index reads: 0
Num index writes: 0
Max Error: 31
Spline max error (0):
Spline points (2):
[0]: (0, 0)
[1]: (992, 0)


Complete.
Stats for 100:
Reads:          0       0
Writes:         3       3
Overwrites:     0       0
Totwrites:      3       3
Buffer hits:    0       0
Write Time:     0       0
R Time:         0       0
R Reads:        6486632 6486632
R Buffer hits:  7679311 7679311
Stats for 200:
Reads:          0       0
Writes:         6       6
Overwrites:     0       0
Totwrites:      6       6
Buffer hits:    0       0
Write Time:     0       0
R Time:         4104    4104
R Reads:        0       0
R Buffer hits:  2       2
Stats for 300:
Reads:          0       0
Writes:         9       9
Overwrites:     0       0
Totwrites:      9       9
Buffer hits:    0       0
Write Time:     0       0
R Time:         6422556 6422556
R Reads:        7667712 7667712
R Buffer hits:  50331648        50331648
Stats for 400:
Reads:          0       0
Writes:         12      12
Overwrites:     0       0
Totwrites:      12      12
Buffer hits:    0       0
Write Time:     0       0
R Time:         48      48
R Reads:        48      48
R Buffer hits:  48      48
Stats for 500:
Reads:          0       0
Writes:         16      16
Overwrites:     0       0
Totwrites:      16      16
Buffer hits:    0       0
Write Time:     0       0
R Time:         0       0
R Reads:        16      16
R Buffer hits:  0       0
Stats for 600:
Reads:          0       0
Writes:         19      19
Overwrites:     0       0
Totwrites:      19      19
Buffer hits:    0       0
Write Time:     0       0
R Time:         4200835 4200835
R Reads:        4201053 4201053
R Buffer hits:  4201269 4201269
Stats for 700:
Reads:          0       0
Writes:         22      22
Overwrites:     0       0
Totwrites:      22      22
Buffer hits:    0       0
Write Time:     0       0
R Time:         7669768 7669768
R Reads:        0       0
R Buffer hits:  4294967294      4294967294
Stats for 800:
Reads:          0       0
Writes:         25      25
Overwrites:     0       0
Totwrites:      25      25
Buffer hits:    0       0
Write Time:     0       0
R Time:         919510542       919510542
R Reads:        0       0
R Buffer hits:  6486680 6486680
Stats for 900:
Reads:          0       0
Writes:         29      29
Overwrites:     0       0
Totwrites:      29      29
Buffer hits:    0       0
Write Time:     0       0
R Time:         1       1
R Reads:        6487012 6487012
R Buffer hits:  2010934892      2010934892
Stats for 1000:
Reads:          0       0
Writes:         33      33
Overwrites:     0       0
Totwrites:      33      33
Buffer hits:    0       0
Write Time:     1       1
R Time:         0       0
R Reads:        33      33
R Buffer hits:  1000    1000
```
### Multiple Source Files

#### Output Size

Output of multiple files added up: 97kb 

#### RESULTS: 

```bash
process_begin: CreateProcess(NULL, uname -s, ...) failed.
makefile:2: pipe: No error
"Running EmbedDB variable data example"
./build/embedDBVariableDataExample.exe

STARTING EmbedDB VARIABLE DATA TESTS.
Initialization success.
EmbedDB State Initialization Stats:
Buffer size: 6  Page size: 512
Key size: 6 Data size: 4 Variable data pointer size: 4 Record size: 14
Use index: 1  Max/min: 0 Sum: 0 Bmap: 1
Header size: 7  Records per page: 36


INSERT TEST:
Elapsed Time: 2 ms
Records inserted: 600
Records with variable data: 600
Num reads: 0
Buffer hits: 0
Num writes: 50
Num index reads: 0
Num index writes: 1
Max Error: 36
Spline max error (0):
Spline points (2):
[0]: (0, 0)
[1]: (576, 0)



QUERY TEST:
Read records: 144
Num: 600 KEY: 26 Perc: 500 Records: 144 Reads: 25
Elapsed Time: 1 ms
Records queried: 600
Fixed records found: 0
Vardata found: 0
Vardata deleted: 0
Num records not found: 0
Num reads: 25
Buffer hits: 726
Num writes: 0
Num index reads: 1
Num index writes: 0
Max Error: 36
Spline max error (0):
Spline points (2):
[0]: (0, 0)
[1]: (576, 0)

Done
Stats for 60:
Reads:          0       0
Writes:         3       3
Overwrites:     0       0
Totwrites:      3       3
Buffer hits:    0       0
Write Time:     0       0
R Time:         1634300513      1634300513
R Reads:        3705242501      3705242501
R Buffer hits:  127     127
Stats for 120:
Reads:          0       0
Writes:         9       9
Overwrites:     0       0
Totwrites:      9       9
Buffer hits:    0       0
Write Time:     0       0
R Time:         1147471017      1147471017
R Reads:        0       0
R Buffer hits:  3       3
Stats for 180:
Reads:          0       0
Writes:         15      15
Overwrites:     0       0
Totwrites:      15      15
Buffer hits:    0       0
Write Time:     0       0
R Time:         9988768 9988768
R Reads:        11      11
R Buffer hits:  0       0
Stats for 240:
Reads:          0       0
Writes:         18      18
Overwrites:     0       0
Totwrites:      18      18
Buffer hits:    0       0
Write Time:     0       0
R Time:         1886216568      1886216568
R Reads:        127     127
R Buffer hits:  5       5
Stats for 300:
Reads:          0       0
Writes:         24      24
Overwrites:     0       0
Totwrites:      24      24
Buffer hits:    0       0
Write Time:     0       0
R Time:         9982472 9982472
R Reads:        9962172 9962172
R Buffer hits:  9982464 9982464
Stats for 360:
Reads:          0       0
Writes:         30      30
Overwrites:     0       0
Totwrites:      30      30
Buffer hits:    0       0
Write Time:     0       0
R Time:         0       0
R Reads:        1       1
R Buffer hits:  9982471 9982471
Stats for 420:
Reads:          0       0
Writes:         33      33
Overwrites:     0       0
Totwrites:      33      33
Buffer hits:    0       0
Write Time:     0       0
R Time:         16842864        16842864
R Reads:        9986512 9986512
R Buffer hits:  6755432 6755432
Stats for 480:
Reads:          0       0
Writes:         39      39
Overwrites:     0       0
Totwrites:      39      39
Buffer hits:    0       0
Write Time:     0       0
R Time:         0       0
R Reads:        3       3
R Buffer hits:  6422476 6422476
Stats for 540:
Reads:          0       0
Writes:         45      45
Overwrites:     0       0
Totwrites:      45      45
Buffer hits:    0       0
Write Time:     0       0
R Time:         9961664 9961664
R Reads:        127     127
R Buffer hits:  6       6
Stats for 600:
Reads:          0       0
Writes:         50      50
Overwrites:     0       0
Totwrites:      50      50
Buffer hits:    0       0
Write Time:     2       2
R Time:         1       1
R Reads:        25      25
R Buffer hits:  726     726
"Finished running EmbedDB variable data example"
```



## Questions: 

What is the point of the #ifndef macro 




