# SBITS Embedded Index Structure for Time Series Data

SBITS is a high performance embedded data storage and index structure for time series data for embedded systems:

1. Uses the minimum of two page buffers for performing all operations. The memory usage is less than 1.5 KB for 512 byte pages.
2. Performance is several times faster than using B-trees and hash-based indexes. Simplifies data management without worrying about low-level details and storing data in raw files.
3. No use of dynamic memory (i.e. malloc()). All memory is pre-allocated at creation of the index.
4. Efficient insert (put) and query (get) of arbitrary key-value data. Ability to search data both on timestamp (key) and by data value.
5. Option to store time series data with or without an index. Adding an index allows for faster retrieval of records based on data value.
6. Support for iterator to traverse data in sorted order.
7. Support for variable-sized records.
8. Easy to use and include in existing projects.
9. Open source license. Free to use for commerical and open source projects.

**Note: This version is designed to run on a PC and may require porting to an embedded device.**


## Documentation 

A paper describing SBITS use for time series indexing is [available from the publisher](https://www.scitepress.org/Link.aspx?doi=10.5220/0010318800920099) and a [pre-print is also available](SBITS_time_series_index.pdf).


#### Table of Contents

-   [Running Examples and Tests](docs/gettingStarted.md)
-   [Usage in your own project](docs/usageInfo.md)
-   [Simple Query Interface](docs/advancedQueries.md)
-   [Building project](docs/buildRunInformation.md)
-   [Test suite](docs/testInfo.md)


## License

[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)