#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "sbits/sbits.h"
#include "sbits/utilityFunctions.h"

void updateCustomUWABitmap(void *data, void *bm);
int8_t inCustomUWABitmap(void *data, void *bm);
void buildCustomUWABitmapFromRange(void *min, void *max, void *bm);

int main() {
    ///////////
    // Setup //
    ///////////
    sbitsState *state = malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 12;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    state->pageSize = 512;
    state->eraseSizeInPages = 4;
    state->numDataPages = 20000;
    state->numIndexPages = 100;
    state->fileInterface = getFileInterface();
    char dataPath[] = "build/artifacts/dataFile.bin", indexPath[] = "build/artifacts/indexFile.bin";
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);
    state->bufferSizeInBlocks = 4;
    state->buffer = calloc(state->bufferSizeInBlocks, state->pageSize);
    state->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX | SBITS_RESET_DATA;
    state->bitmapSize = 2;
    state->inBitmap = inCustomUWABitmap;
    state->updateBitmap = updateCustomUWABitmap;
    state->buildBitmapFromRange = buildCustomUWABitmapFromRange;
    sbitsInit(state, 1);

    char *recordBuffer = malloc(state->recordSize);

    ////////////////////////
    // Insert uwa dataset //
    ////////////////////////
    printf("\nINSERT\n");
    clock_t start = clock();

    int numRecords = 0;
    uint32_t minKey = 946713600;
    uint32_t maxKey = 977144040;

    FILE *dataset = fopen("data/uwa500K.bin", "rb");
    char dataPage[512];
    while (fread(dataPage, 512, 1, dataset)) {
        uint16_t count = *(uint16_t *)(dataPage + 4);
        for (int record = 1; record <= count; record++) {
            sbitsPut(state, dataPage + record * state->recordSize, dataPage + record * state->recordSize + state->keySize);
            numRecords++;
        }
    }
    sbitsFlush(state);

    clock_t timeInsert = clock() - start;

    printf("Num Records inserted: %d\n", numRecords);
    printf("Time: %dms\n", timeInsert);
    printf("Num data pages written: %d\n", state->numWrites);
    printf("Num index pages written: %d\n", state->numIdxWrites);

    ////////////////
    // SELECT All //
    ////////////////
    printf("\nSELECT * FROM r\n");
    start = clock();

    sbitsIterator itSelectAll;
    itSelectAll.minKey = NULL;
    itSelectAll.maxKey = NULL;
    itSelectAll.minData = NULL;
    itSelectAll.maxData = NULL;
    sbitsInitIterator(state, &itSelectAll);

    int numRecordsSelectAll = 0;
    int numReadsSelectAll = state->numReads;

    while (sbitsNext(state, &itSelectAll, recordBuffer, recordBuffer + state->keySize)) {
        numRecordsSelectAll++;
    }

    clock_t timeSelectAll = clock() - start;

    numReadsSelectAll = state->numReads - numReadsSelectAll;

    printf("Result size: %d\n", numRecordsSelectAll);
    printf("Time: %dms\n", timeSelectAll);
    printf("Num reads: %d\n", numReadsSelectAll);

    /////////////////////////////////
    // SELECT by key, small result //
    /////////////////////////////////
    printf("\nSELECT by key, small result\n");
    start = clock();

    sbitsIterator itSelectKeySmallResult;
    uint32_t minKeySelectKeySmallResult = maxKey - (maxKey - minKey) * 0.1;
    itSelectKeySmallResult.minKey = &minKeySelectKeySmallResult;
    itSelectKeySmallResult.maxKey = NULL;
    itSelectKeySmallResult.minData = NULL;
    itSelectKeySmallResult.maxData = NULL;
    sbitsInitIterator(state, &itSelectKeySmallResult);

    int numRecordsSelectKeySmallResult = 0;
    int numReadsSelectKeySmallResult = state->numReads;

    while (sbitsNext(state, &itSelectKeySmallResult, recordBuffer, recordBuffer + state->keySize)) {
        numRecordsSelectKeySmallResult++;
    }

    clock_t timeSelectKeySmallResult = clock() - start;

    numReadsSelectKeySmallResult = state->numReads - numReadsSelectKeySmallResult;

    printf("Result size: %d\n", numRecordsSelectKeySmallResult);
    printf("Time: %dms\n", timeSelectKeySmallResult);
    printf("Num reads: %d\n", numReadsSelectKeySmallResult);

    /////////////////////////////////
    // SELECT by key, large result //
    /////////////////////////////////
    printf("\nSELECT by key, large result\n");
    start = clock();

    sbitsIterator itSelectKeyLargeResult;
    uint32_t minKeySelectKeyLargeResult = maxKey - (maxKey - minKey) * 0.9;
    itSelectKeyLargeResult.minKey = &minKeySelectKeyLargeResult;
    itSelectKeyLargeResult.maxKey = NULL;
    itSelectKeyLargeResult.minData = NULL;
    itSelectKeyLargeResult.maxData = NULL;
    sbitsInitIterator(state, &itSelectKeyLargeResult);

    int numRecordsSelectKeyLargeResult = 0;
    int numReadsSelectKeyLargeResult = state->numReads;

    while (sbitsNext(state, &itSelectKeyLargeResult, recordBuffer, recordBuffer + state->keySize)) {
        numRecordsSelectKeyLargeResult++;
    }

    clock_t timeSelectKeyLargeResult = clock() - start;

    numReadsSelectKeyLargeResult = state->numReads - numReadsSelectKeyLargeResult;

    printf("Result size: %d\n", numRecordsSelectKeyLargeResult);
    printf("Time: %dms\n", timeSelectKeyLargeResult);
    printf("Num reads: %d\n", numReadsSelectKeyLargeResult);

    //////////////////////////////////////////////////////////
    // SELECT * FROM r WHERE key >= 60000 AND key <= 360000 //
    //////////////////////////////////////////////////////////

    /////////////////////////////////////////////
    // SELECT * FROM r WHERE data >= 950000000 //
    /////////////////////////////////////////////

    /////////////////////////////////////////////
    // SELECT * FROM r WHERE data >= 970000000 //
    /////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////
    // SELECT * FROM r WHERE data >= 950000000 AND data <= 970000000 //
    ///////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////
    // SELECT * FROM r WHERE key <= 400000 AND data >= 950000000 //
    ///////////////////////////////////////////////////////////////

    /////////////////
    // Close SBITS //
    /////////////////
    sbitsClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
    free(state->fileInterface);
    free(state->buffer);
    free(recordBuffer);
}

void updateCustomUWABitmap(void *data, void *bm) {
    int32_t temp = *(int32_t *)data;

    int32_t min = 303;
    int32_t max = 892;

    int32_t index = (float)(temp - min) / (max - min) * 16;

    if (index < 0) index = 0;
    if (index > 15) index = 15;

    uint16_t mask = 1;
    mask <<= index;
    *(uint16_t *)bm |= mask;
}

int8_t inCustomUWABitmap(void *data, void *bm) {
    uint16_t *bmval = (uint16_t *)bm;

    uint16_t tmpbm = 0;
    updateCustomUWABitmap(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return (tmpbm & *bmval) > 0;
}

void buildCustomUWABitmapFromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint16_t *)bm = 65535; /* Everything */
        return;
    } else {
        uint16_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateCustomUWABitmap(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint16_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateCustomUWABitmap(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = ~(maxMap - 1);
            if (min == NULL) {
                *(uint16_t *)bm = maxMap;
                return;
            }
        }
        *(uint16_t *)bm = minMap & maxMap;
    }
}
