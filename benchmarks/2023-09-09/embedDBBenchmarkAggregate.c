/******************************************************************************/
/**
 * @file		advancedQueryInterfaceExample.c
 * @author		EmbedDB Team (See Authors.md)
 * @brief		This file includes an example of querying EmbedDB using the
 *              advanced query interface.
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

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "../../src/embedDB/embedDB.h"
#include "../../src/embedDB/utilityFunctions.h"
#include "../../src/query-interface/advancedQueries.h"

uint32_t dayGroup(const void* record) {
    // find the epoch day
    return (*(uint32_t*)record) / 86400;
}

int8_t sameDayGroup(const void* lastRecord, const void* record) {
    return dayGroup(lastRecord) == dayGroup(record);
}

void writeDayGroup(embedDBAggregateFunc* aggFunc, embedDBSchema* schema, void* recordBuffer, const void* lastRecord) {
    // Put day in record
    uint32_t day = dayGroup(lastRecord);
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(schema, aggFunc->colNum), &day, sizeof(uint32_t));
}

void insertData(embedDBState* state, char* filename);

int main() {
#define numRuns 3
    int32_t times[numRuns];
    int32_t numReads[numRuns];
    int32_t numIdxReads[numRuns];
    int32_t numRecords[numRuns];

    for (int run = 0; run < numRuns; run++) {
        // Setup embedDB
        embedDBState* stateUWA = calloc(1, sizeof(embedDBState));
        stateUWA->keySize = 4;
        stateUWA->dataSize = 12;
        stateUWA->compareKey = int32Comparator;
        stateUWA->compareData = int32Comparator;
        stateUWA->pageSize = 512;
        stateUWA->eraseSizeInPages = 4;
        stateUWA->numDataPages = 20000;
        stateUWA->numIndexPages = 1000;
        stateUWA->numSplinePoints = 300;
        char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
        stateUWA->fileInterface = getFileInterface();
        stateUWA->dataFile = setupFile(dataPath);
        stateUWA->indexFile = setupFile(indexPath);
        stateUWA->bufferSizeInBlocks = 4;
        stateUWA->buffer = malloc(stateUWA->bufferSizeInBlocks * stateUWA->pageSize);
        stateUWA->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
        stateUWA->bitmapSize = 2;
        stateUWA->inBitmap = inBitmapInt16;
        stateUWA->updateBitmap = updateBitmapInt16;
        stateUWA->buildBitmapFromRange = buildBitmapInt16FromRange;
        embedDBInit(stateUWA, 1);

        int8_t colSizes[] = {4, 4, 4, 4};
        int8_t colSignedness[] = {embedDB_COLUMN_UNSIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED, embedDB_COLUMN_SIGNED};
        embedDBSchema* baseSchema = embedDBCreateSchema(4, colSizes, colSignedness);

        // Insert data
        insertData(stateUWA, "../../data/uwa500K_only_100K.bin");

        // Start benchmark
        numReads[run] = stateUWA->numReads;
        numIdxReads[run] = stateUWA->numIdxReads;
        times[run] = clock();

        /* Aggregate Query: min, max, and avg temperature for each day */
        embedDBIterator it;
        it.minKey = NULL;
        it.maxKey = NULL;
        it.minData = NULL;
        it.maxData = NULL;
        embedDBInitIterator(stateUWA, &it);

        embedDBOperator* scanOp = createTableScanOperator(stateUWA, &it, baseSchema);
        embedDBAggregateFunc groupName = {NULL, NULL, writeDayGroup, NULL, 4};
        embedDBAggregateFunc* minTemp = createMinAggregate(1, -4);
        embedDBAggregateFunc* maxTemp = createMaxAggregate(1, -4);
        embedDBAggregateFunc* avgTemp = createAvgAggregate(1, 4);
        embedDBAggregateFunc aggFunctions[] = {groupName, *minTemp, *maxTemp, *avgTemp};
        uint32_t functionsLength = 4;
        embedDBOperator* aggOp = createAggregateOperator(scanOp, sameDayGroup, aggFunctions, functionsLength);
        aggOp->init(aggOp);

        int32_t recordsReturned = 0;
        int32_t printLimit = 10000;
        int32_t* recordBuffer = aggOp->recordBuffer;
        // printf("\nCount Result:\n");
        // printf("Day   | Min Temp | Max Temp | Avg Temp\n");
        // printf("------+----------+----------+----------\n");
        while (exec(aggOp)) {
            recordsReturned++;
            // if (recordsReturned < printLimit) {
            //     printf("%5lu | %8.1f | %8.1f | %8.1f\n", recordBuffer[0], recordBuffer[1] / 10.0, recordBuffer[2] / 10.0, *((float*)recordBuffer + 3) / 10.0);
            // }
        }
        // if (recordsReturned > printLimit) {
        //     printf("...\n");
        //     printf("[Total records returned: %d]\n", recordsReturned);
        // }

        // End benchmark
        times[run] = (clock() - times[run]) / CLOCKS_PER_SEC;
        numReads[run] = stateUWA->numReads - numReads[run];
        numIdxReads[run] = stateUWA->numIdxReads - numIdxReads[run];
        numRecords[run] = recordsReturned;

        // Free states
        for (int i = 0; i < functionsLength; i++) {
            if (aggFunctions[i].state != NULL) {
                free(aggFunctions[i].state);
            }
        }

        aggOp->close(aggOp);
        embedDBFreeOperatorRecursive(&aggOp);

        // Close embedDB
        embedDBClose(stateUWA);
        tearDownFile(stateUWA->dataFile);
        tearDownFile(stateUWA->indexFile);
        free(stateUWA->fileInterface);
        free(stateUWA->buffer);
        free(stateUWA);
    }

    // Print results
    int sum = 0;
    printf("\nAggregate query\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", times[i]);
        sum += times[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num records returned: %d\n", numRecords[0]);
    printf("Num data pages read: %d\n", numReads[0]);
    printf("Num index pages read: %d\n", numIdxReads[0]);
}

void insertData(embedDBState* state, char* filename) {
    FILE* fp = fopen(filename, "rb");
    char fileBuffer[512];
    int numRecords = 0;
    while (fread(fileBuffer, state->pageSize, 1, fp)) {
        uint16_t count = EMBEDDB_GET_COUNT(fileBuffer);
        for (int i = 1; i <= count; i++) {
            embedDBPut(state, fileBuffer + i * state->recordSize, fileBuffer + i * state->recordSize + state->keySize);
            numRecords++;
        }
    }
    fclose(fp);
    embedDBFlush(state);
}
