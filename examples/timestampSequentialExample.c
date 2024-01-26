/******************************************************************************/
/**
 * @file		embedDBExample.c
 * @author		EmbedDB Team (See Authors.md)
 * @brief		This file includes a simple example for insterting and retrieving timestamp records for EmbeDB.
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
#include <inttypes.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "../src/embedDB/embedDB.h"
#include "../src/embedDB/utilityFunctions.h"

int insertFixedRecordsWithTimestamp(uint32_t n);
int queryPrintFixedRecords(uint64_t start, uint64_t end);
embedDBState* init_state();

embedDBState* state;

int main() {
    printf("*********Example of EmbeDB with Sequential Timestamp Key Records*********\n");
    state = init_state();
    embedDBPrintInit(state);

    // Inserting fixed-data records with timestamp keys
    printf("*********Inserting and Retrieving Fixed-Data in the Write Buffer*********\n");
    int n = 10;  // Number of records to insert
    if (insertFixedRecordsWithTimestamp(n) == -1) {
        printf("Error inserting records\n");
        exit(0);
    }

    // Querying fixed-data records
    printf("Querying %d records from write buffer using iterator\n", n);
    uint64_t currentTime = (uint64_t)time(NULL);
    // queryPrintFixedRecords(currentTime - n, currentTime);

    embedDBIterator it;

    uint64_t* itKey;
    uint64_t* itData[] = {0, 0, 0};
    uint64_t minData = currentTime - n, maxData = currentTime;

    it.minKey = &minData;
    it.maxKey = &maxData;
    it.minData = NULL;
    it.maxData = NULL;

    embedDBInitIterator(state, &it);

    while (embedDBNext(state, &it, (void**)&itKey, (void**)&itData)) {
        printf("key = %d, data = %d\n", itKey, *itData);
    }

    printf("Flushed\n");
    embedDBFlush(state);

    printf("Done.\n");
}

int insertFixedRecordsWithTimestamp(uint32_t n) {
    printf("Inserting %u records using timestamp keys...\n", n);

    uint64_t* data = calloc(1, sizeof(uint64_t));
    if (data == NULL) {
        return -1;  // Memory allocation failed
    }

    for (uint32_t i = 0; i < n; i++) {
        uint64_t timestampKey = (uint64_t)time(NULL);
        *data = i % 100;  // Example data
        int result = embedDBPut(state, &timestampKey, data);
        printf("Inserted key = %" PRIu64 " and data = %" PRIu64 "\n", timestampKey, *data);
        if (result != 0) {
            free(data);
            return result;
        }
        sleep(1);
    }

    free(data);
    return 0;
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