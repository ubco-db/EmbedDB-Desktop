# Usage & Setup Documentation

## Table of Contents

-   [Configure EmbedDB State](#configure-records)
    -   [Create an EmbedDB state](#create-an-embedDB-state)
    -   [Size of records](#configure-size-of-records)
    -   [Comparator Functions](#comparator-functions)
    -   [Storage Addresses](#configure-storage-addresses)
    -   [Memory Buffers](#configure-memory-buffers)
    -   [Other Parameters](#other-parameters)
    -   [Final Initialization](#final-initilization)
-   [Setup Index](#setup-index-method-and-optional-radix-table)
-   [Insert Records](#insert-put-items-into-table)
-   [Query Records](#query-get-items-from-table)
-   [Iterate over Records](#iterate-through-items-in-tree)
    -   [Filter by key](#iterator-with-filter-on-keys)
    -   [Filter by data](#iterator-with-filter-on-data)
    -   [Iterate with vardata](#iterate-over-records-with-vardata)
-   [Disposing of EmbedDB state](#disposing-of-embedDB-state)

## Configure Records

### Create an EmbedDB state

```c
embedDBState* state = (embedDBState*) malloc(sizeof(embedDBState));
```

### Configure size of records

These attributes are only for fixed-size data/keys. If you require variable-sized records, see below. 

```c
state->keySize = 4;  // Allowed up to 8
state->dataSize = 12;  // No limit as long as at least one record can fit on a page after the page header
```
**Note** `state->recordsize` is not necessarily the sum of `state->keySize` and `state->dataSize`. If you have variable data enabled, a 4-byte pointer exists in the fixed record that points to the record in located variable storage.

### Comparator functions

You can find example implementations of these functions in src/embedDB/utilityFunctions.c \

These can be customized for your own keys and data.


```c
// Function pointers that can compare two keys/data
state->compareKey = int32Comparator;
state->compareData = dataComparator;
```

### Configure file storage

Configure the number of bytes per page and the minimum erase size for your storage medium.

```c
state->pageSize = 512;
state->eraseSizeInPages = 4;
```

Set the number of allocated file pages for each of the relevant files.

```c
state->numDataPages = 1000;
state->numIndexPages = 48;
state->numVarPages = 1000;
```

*Note: it is not necessary to allocate the index and varPages pages if you are not using them.* 

Setup the file interface that allows EmbedDB to work with any storage device. More info on: [Setting up a file interface](fileInterface.md).

```c
char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin", varPath[] = "varFile.bin";
state->fileInterface = getSDInterface();
state->dataFile = setupSDFile(dataPath);
state->indexFile = setupSDFile(indexPath);
state->varFile = setupSDFile(varPath);
```

### Configure memory buffers

How to choose the number of blocks to use: 
-   Required:
    -   2 blocks for record read/write buffers
-   Optional:
    -   2 blocks for index read/write buffers (Writing the bitmap index to file)
    -   2 blocks for variable data read/write buffers (If you need to have a variable sized portion of the record) 

```c
state->bufferSizeInBlocks = 6; //6 buffers is needed when using index and variable
state->buffer = malloc((size_t) state->bufferSizeInBlocks * state->pageSize);
```

### Other parameters:

```c
state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_USE_VDATA;
```

`EMBEDDB_USE_INDEX` - Writes the bitmap to a file for fast queries on the data (Usually used in conjuction with EMBEDDB_USE_BMAP). \
`EMBEDDB_USE_BMAP` - Includes the bitmap in each page header so that it is easy to tell if a buffered page may contain a given key. \
`EMBEDDB_USE_MAX_MIN` - Includes the max and min records in each page header. \
`EMBEDDB_USE_VDATA` - Enables including variable-sized data with each record. \
`EMBEDDB_RESET_DATA` - Disables data recovery.

*Note: If `EMBEDDB_RESET_DATA` is not enabled, embedDB will check if the file already exists, and if it does, it will attempt at recovering the data.*

### Bitmap 

// probably need some notes here. 


```c
state->bitmapSize = 8;

// Include function pointers that can build/update a bitmap of the specified size
state->inBitmap = inBitmapInt64;
state->updateBitmap = updateBitmapInt64;
state->buildBitmapFromRange = buildBitmapInt64FromRange;
```


### Final initilization

```c
size_t splineMaxError = 1; // Modify this value to change spline error tolerance
embedDBInit(state, splineMaxError);
```

## Setup Index Method and Optional Radix Table

```c
// In embedDB.c
#define SEARCH_METHOD 2
#define RADIX_BITS 0
#define ALLOCATED_SPLINE_POINTS 300
```

The `SEARCH_METHOD` defines the method used for indexing physical data pages.

-   0 Uses a linear function to approximate data page locations.
-   1 Performs a binary search over all data pages.
-   **2 Uses a Spline structure (with optional Radix table) to index data pages. _This is the recommended option_**

The `RADIX_BITS` constant defines how many bits are indexed by the Radix table when using `SEARCH_METHOD 2`.
Setting this constant to 0 will omit the Radix table, and indexing will rely solely on the Spline structure.

`ALLOCATED_SPLINE_POINTS` sets how many spline points will be allocated during initialization. This is a set amount and will not grow as points are added. The amount you need will depend on how much your key rate varies and what `maxSplineError` is set to during embedDB initialization.

## Insert (put) items into table

`key` is the key to insert. `dataPtr` points to associated data value. Keys are always assumed to be unsigned numbers and **must always be inserted in ascending order.**

#### Inserting Fixed-Size Data

Method:

```c
embedDBGet(embedDBState *state, void *key, void *data) 
```

Example:

```c
// Initialize key (note keys must always be in ascending order)
uint32_t key = 123;
// calloc dataPtr in the heap
void* dataPtr = calloc(1, state->dataSize);
// set value to be inserted into embedDB
*((uint32_t*)data) = 12345678;
// perform insert of key and data 
embedDBPut(state, (void*) &key, dataPtr);
// free dynamic memory
free(dataPtr);
```

*Note: void pointers here are used to utilize different data-types*

#### Inserting Variable-Length Data

embedDB has support for variable length records, but only when `EMBEDDB_USE_VDATA` is enabled. `varPtr` points to the variable sized data that you would like to insert and `length` specifies how many bytes that record takes up. It is important to note that when inserting variable-length data, embedDB still inserts fixed-size records as shown above, alongside the variable-length record that you would like to insert. If an individual record does not have any variable data, simply set `varPtr = NULL` and `length = 0`.

Method:

```c 
embedDBPutVar(state, (void*) &key, (void*) dataPtr, (void*) varPtr, (uint32_t) length);
```

Example:

```c
// init key 
uint32_t key = 124; 
// Fixed size records can be inserted alongside variable records. 
void* dataPtr = calloc(1, state->dataSize);
// Create variable-length data
char varPtr[] = "Hello World"; // ~ 12 bytes long 
// specify the length in bytes
uint32_t length = 12;
// insert variable record 
embedDBPutVar(state, &key, &dataPtr, varPtr, length);
```

Flush the EmbedDB buffer to write the currently buffered page to storage. 

```c
embedDBFlush(state);
```

## Query (get) items from table

_For a simpler query interface, see [Simple Query Interface](advancedQueries.md)_

`keyPtr` points to key to search for. `dataPtr` must point to pre-allocated space to copy data into.

### Fixed-Length Record

Method:

```c
// could use some more code here 
embedDBGet(state, (void*) keyPtr, (void*) dataPtr);
```

Example: 

### Variable-Length Record


If the `EMBEDDB_USE_VDATA` parameter is used, then a variable data stream needs to be created to read the variable length record. `varStream` is an un-allocated `embedDBVarDataStream`.  This method will return a data stream if there is variable data to read. Variable data is read in chunks from the stream. `bytesRead` is the number of bytes read into the buffer and is <=`varBufSize`. 

```c
embedDBVarDataStream *varStream = NULL;
uint32_t varBufSize = 8;  // The size is varaible but must be at least the size of the retrieved record. 
void *varDataBuffer = malloc(varBufSize);

embedDBGetVar(state, (void*) keyPtr, (void*) dataPtr, &varStream);

if (varStream != NULL) {
	uint32_t bytesRead;
	while ((bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuf, varBufSize)) > 0) {
		// Process data in varDataBuf
	}
	free(varStream);
	varStream = NULL;
}
```

## Iterate through items in table

### Iterator with filter on keys

```c
// Recall, these are our record sizes set previously. 
state->keySize = 4;  
state->dataSize = 12; 

embedDBIterator it;
uint32_t *itKey;
// Allocate memory for itData matching the size of state->datasize
uint32_t* itData = malloc(3 * sizeof(uint32_t)); 
void *varDataBuffer = malloc(varBufSize);

uint32_t minKey = 1, maxKey = 1000;
it.minKey = &minKey;
it.maxKey = &maxKey;
it.minData = NULL;
it.maxData = NULL;

embedDBInitIterator(state, &it);

while (embedDBNext(state, &it, (void**) &itKey, (void**) &itData)) {
	/* Process record */
}

embedDBCloseIterator(&it);
```

### Iterator with filter on data

```c
// Same setup as above

it.minKey = NULL;
it.maxKey = NULL;
uint32_t minData = 90, maxData = 100;
it.minData = &minData;
it.maxData = &maxData;

embedDBInitIterator(state, &it);

while (embedDBNext(state, &it, (void**) &itKey, (void**) &itData)) {
	/* Process record */
}

embedDBCloseIterator(&it);
```

### Iterate over records with vardata

```c
uint32_t minKey = 23, maxKey = 356;
it.minKey = &minKey;
it.maxKey = &maxKey;
it.minData = NULL;
it.maxData = NULL;

embedDBVarDataStream *varStream = NULL;
uint32_t varBufSize = 8;  // Choose any size
void *varDataBuffer = malloc(varBufSize);

embedDBInitIterator(state, &it);

while (embedDBNextVar(state, &it, &itKey, itData, &varStream)) {
	/* Process fixed part of record */
	...
	/* Process vardata if this record has it */
	if (varStream != NULL) {
		uint32_t numBytesRead = 0;
		while ((numBytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize)) > 0) {
			/* Process the data read into the buffer */
			for (uint32_t i = 0; i < numBytesRead; i++) {
				printf("%02X ", (uint8_t*)varDataBuffer + i);
			}
		}
		printf("\n");

		free(varStream);
		varStream = NULL;
	}
}

free(varDataBuffer);
embedDBCloseIterator(&it);
```

## Disposing of EmbedDB state

**Be sure to flush buffers before closing, if needed.**

```c
embedDBClose(state);
tearDownFile(state->dataFile); 
tearDownFile(state->indexFile);	
tearDownFile(state->varFile);
free(state->fileInterface);
free(state->buffer);
free(state);
```

*Note: Only tearDown files specified in `state->parameters`.*
