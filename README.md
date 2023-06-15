# SBITS Embedded Index Structure for Time Series Data 

SBITS is a high performance embedded data storage and index structure for time series data for embedded systems:

1. Uses the minimum of two page buffers for performing all operations. The memory usage is less than 1.5 KB for 512 byte pages.
2. Performance is several times faster than using B-trees and hash-based indexes. Simplifies data management without worrying about low-level details and storing data in raw files.
3. No use of dynamic memory (i.e. malloc()). All memory is pre-allocated at creation of the index.
4. Efficient insert (put) and query (get) of arbitrary key-value data. Ability to search data both on timestamp (key) and by data value.
5. Option to store time series data with or without an index. Adding an index allows for faster retrieval of records based on data value.
6. Support for iterator to traverse data in sorted order.
7. Easy to use and include in existing projects. 
8. Open source license. Free to use for commerical and open source projects.

**Note: This version is designed to run on a PC and may require porting to an embedded device.**

## License
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

## Code Files

* test_sbits.h - test file demonstrating how to get, put, and iterate through data in index
* main.cpp - main Arduino code file
* sbits.h, sbits.c - implementation of SBITS index structure supporting arbitrary key-value data items

## Documentation

A paper describing SBITS use for time series indexing is [available from the publisher](https://www.scitepress.org/Link.aspx?doi=10.5220/0010318800920099) and a [pre-print is also available](SBITS_time_series_index.pdf).

## Usage

### Configure Records
Create an sbits state
```c
sbitsState* state = (sbitsState*) malloc(sizeof(sbitsState));
```
Configure the size of the records. These attributes are only for fixed-size data/keys. If you require variable-sized records keep reading
```c
state->keySize = 4;  // Allowed up to 8
state->dataSize = 12;        
```
Configure storage addresses
```c
state->pageSize = 512;
state->eraseSizeInPages = 4;

/* If your storage medium has a file system (e.g. sd card) then the start address doesn't matter, only the different between the start and end.
 * If you do not have a file system, then be sure that you do not overlap memory regions.
 */
state->startAddress = 0;
state->endAddress = 1000 * state->pageSize;

// If variable sized data will be stored
state->varAddressStart = 2000 * state->pageSize;
state->varAddressEnd = state->varAddressStart + 1000 * state->pageSize;
```
Configure memory buffers
 ```c
/* How to choose the number of blocks to use: 
 * Required: 
 *     - 2 blocks for record read/write buffers
 * Optional:
 *     - 2 blocks for index read/write buffers (Writing the bitmap index to file)
 *     - 2 blocks for variable data read/write buffers (If you need to have a variable sized portion of the record)
 */
state->bufferSizeInBlocks = 6;
state->buffer = malloc((size_t) state->bufferSizeInBlocks * state->pageSize);
```
Other parameters:
```c
state->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX | SBITS_USE_VDATA;
```
`SBITS_USE_INDEX` - Writes the bitmap to a file for fast queries on the data (Usually used in conjuction with `SBITS_USE_BMAP`) \
`SBITS_USE_BMAP` - Includes the bitmap in each page header so that it is easy to tell if a buffered page may contain a given key 
```c
state->bitmapSize = 8;

// Include function pointers that can build/update a bitmap of the specified size
state->inBitmap = inBitmapInt64;
state->updateBitmap = updateBitmapInt64;
state->buildBitmapFromRange = buildBitmapInt64FromRange;
```

`SBITS_USE_MAX_MIN` - Includes the max and min records in each page header \
`SBITS_USE_VDATA` - Enables including variable-sized data with each record
```c

state->compareKey = int32Comparator;
state->compareData = int32Comparator;

/* Initialize SBITS structure with parameters */
size_t splineMaxError = 1; /* Modify this value to change spline error tolerance */
sbitsInit(state, splineMaxError);
```

### Setup Index

### Setup Index Method and Optional Radix Table
```c
// In sbits.c

/**
 * 0 = Value-based search
 * 1 = Binary serach
 * 2 = Modified linear search (Spline)
 */
#define SEARCH_METHOD 2

/**
 * Number of bits to be indexed by the Radix Search structure
 * Note: The Radix search structure is only used with Spline (SEARCH_METHOD == 2)
 * To use a pure Spline index without a Radix table, set RADIX_BITS to 0
 */
#define RADIX_BITS 4

```

The SEARCH_METHOD defines the method used for indexing physical data pages.
* 0 Uses a linear function to approximate data page locations.
* 1 Performs a binary search over all data pages.
* 2 Uses a Spline structure (with optional Radix table) to index data pages.

The RADIX_BITS constant defines how many bits are indexed by the Radix table when using SEARCH_METHOD 2.
Setting this constant to 0 will omit the Radix table, indexing will rely solely on the Spline structure.

### Insert (put) items into tree

```c
/* keyPtr points to key to insert. dataPtr points to associated data value. */
sbitsPut(state, (void*) keyPtr, (void*) dataPtr);

/* flush the sbits buffer when done inserting points. */
sbitsFlush(state);
```

### Query (get) items from tree

```c
/* keyPtr points to key to search for. dataPtr must point to pre-allocated space to copy data into. */
int8_t result = sbitsGet(state, (void*) keyPtr, (void*) dataPtr);
```

### Iterate through items in tree

```c
/* Iterator with filter on keys */
sbitsIterator it;
int32_t *itKey, *itData;

uint32_t minKey = 1, maxKey = 1000;     
it.minKey = &minKey; 
it.maxKey = &maxKey;
it.minData = NULL;
it.maxData = NULL;    

sbitsInitIterator(state, &it);

while (sbitsNext(state, &it, (void**) &itKey, (void**) &itData))
{                      
	/* Process record */	
}

/* Iterator with filter on data */       
it.minKey = NULL;    
it.maxKey = NULL;
uint32_t minData = 90, maxData = 100;  
it.minData = &minData;
it.maxData = &maxData;    

sbitsInitIterator(state, &it);

while (sbitsNext(state, &it, (void**) &itKey, (void**) &itData))
{                      
	/* Process record */	
}
```
#### Ramon Lawrence, David Ding, Ivan Carvalho<br>University of British Columbia Okanagan
