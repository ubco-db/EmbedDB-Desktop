/******************************************************************************/
/**
 * @file		embedDBExample.c
 * @author		EmbedDB Team (See Authors.md)
 * @brief		This file includes and example for insterting and retrieving sequential records for EmbeDB.
 * @copyright	Copyright 2023
 * 			    EmbedDB Team
 * @par Redistribution and use in source and binary forms, with or without
 * 	modification, are permitted provided that the following conditions are met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 * 	this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 * 	this list of conditions and the following disclaimer in the documentation
 * 	and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its contributors
 * 	may be used to endorse or promote products derived from this software without
 * 	specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * 	AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * 	IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * 	ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * 	LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * 	CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * 	SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * 	INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * 	CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * 	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * 	POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/

#include <errno.h>
#include <string.h>
#include <time.h>

#include "../src/embedDB/embedDB.h"
#include "../src/embedDB/utilityFunctions.h"

int insertFixedRecords(uint32_t n);
int queryPrintFixedRecords(uint32_t start, uint32_t end);
embedDBState* init_state();

embedDBState* state;

int totalRecords = 0;

int main() {
    printf("****************Performing an example of EmbeDB with sequentially generated data****************\n");
    state = init_state();
    embedDBPrintInit(state);

    printf("*******************For inserting and retrieving fixed-data in the write buffer*******************\n");
    int n = 10;  // insert 10 records
    if (insertFixedRecords(n) == -1) {
        printf("There was an error inserting static records\n");
        exit(0);
    }
    printf("Querying %d records from write buffer using embeDDBGet()...\n", n);
    queryPrintFixedRecords(0, 10);

    printf("******For retrieving records in the file system and iterating over them with the iterator*******\n");
    printf("Flushing embedDB so the files are in the file system...\n");
    embedDBFlush(state);
    printf("Flushed\n");
    n = 25;  // insert 25 more records
    if (insertFixedRecords(n) == -1) {
        printf("There was an error inserting static records\n");
        exit(0);
    }
    printf("Iterating over keys, 10 -> 34 inclusive...\n");

    embedDBIterator it;

    uint32_t* itKey;
    uint32_t* itData[] = {0, 0, 0};
    uint32_t minKey = 10, maxKey = 34;

    it.minKey = &minKey;
    it.maxKey = &maxKey;
    it.minData = NULL;
    it.maxData = NULL;

    embedDBInitIterator(state, &it);

    while (embedDBNext(state, &it, (void**)&itKey, (void**)&itData)) {
        printf("Iterated key = %d and data = %d\n", itKey, *itData);
    }

    printf("Iterating over data, 10 -> 34 inclusive...\n");

    // itKey;
    // itData = {0, 0, 0};
    uint32_t minData = 10, maxData = 34;

    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = &minData;
    it.maxData = &maxData;

    embedDBInitIterator(state, &it);

    while (embedDBNext(state, &it, (void**)&itKey, (void**)&itData)) {
        printf("key = %d and iterated data = %d\n", itKey, *itData);
    }

    // initalize buffer variables.

    // INSERT VARIALBE LENGTH RECORDS

    // retrieve variable length records

    // RETRIEVE VARIABLE LENGTH REOCRS WITH ITERATOR?

    // it would be nice to print the variable records.
}

insertFixedRecords(uint32_t n) {
    printf("Inserting %d records using embedDBPut()...\n", n);
    int targetNum = n + totalRecords;
    for (uint64_t i = totalRecords; i < targetNum; i++) {
        uint64_t data = i % 100;
        int result = embedDBPut(state, &i, &data);
        printf("Inserted key = %d and data = %d\n", i, data);
        if (result != 0) {
            return result;
        }
        totalRecords++;
    }
    return 0;
}

int queryPrintFixedRecords(uint32_t start, uint32_t end) {
    // retrieve records from start to end
    for (int i = start; i < end; i++) {
        // key for data retrieval
        int key = i;
        // data pointer for retrieval
        int returnDataPtr[] = {0, 0, 0};
        // query embedDB
        embedDBGet(state, (void*)&key, (void*)returnDataPtr);
        // print result
        printf("Returned key = %d data = %d\n", key, *returnDataPtr);
    }
}

embedDBState* init_state() {
    state = (embedDBState*)malloc(sizeof(embedDBState));
    if (state == NULL) {
        printf("Unable to allocate state. Exiting\n");
        exit(0);
    }
    // configure state variables
    state->keySize = 4;
    state->dataSize = 8;
    state->pageSize = 512;
    state->numSplinePoints = 300;
    state->bitmapSize = 1;
    state->bufferSizeInBlocks = 6;
    // allocate buffer
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    // check
    if (state->buffer == NULL) {
        printf("Unable to allocate buffer. Exciting\n");
        exit(0);
    }
    // address level parameters
    state->numDataPages = 1000;
    state->numIndexPages = 48;
    state->numVarPages = 75;
    state->eraseSizeInPages = 4;
    // configure file interface
    char dataPath[] = "build/artifacts/dataFile.bin", indexPath[] = "build/artifacts/indexFile.bin", varPath[] = "build/artifacts/varFile.bin";
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);
    state->varFile = setupFile(varPath);
    // configure state
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_USE_VDATA | EMBEDDB_RESET_DATA;
    // Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    // init
    size_t splineMaxError = 1;

    if (embedDBInit(state, splineMaxError) != 0) {
        printf("Initialization error");
        exit(0);
    }

    embedDBResetStats(state);
}
