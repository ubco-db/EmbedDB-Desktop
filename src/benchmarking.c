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

    clock_t timeInsert = (clock() - start) / (CLOCKS_PER_SEC / 1000);

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

    clock_t timeSelectAll = (clock() - start) / (CLOCKS_PER_SEC / 1000);

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

    clock_t timeSelectKeySmallResult = (clock() - start) / (CLOCKS_PER_SEC / 1000);

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

    clock_t timeSelectKeyLargeResult = (clock() - start) / (CLOCKS_PER_SEC / 1000);

    numReadsSelectKeyLargeResult = state->numReads - numReadsSelectKeyLargeResult;

    printf("Result size: %d\n", numRecordsSelectKeyLargeResult);
    printf("Time: %dms\n", timeSelectKeyLargeResult);
    printf("Num reads: %d\n", numReadsSelectKeyLargeResult);

    //////////////////////////
    // SELECT range of keys //
    //////////////////////////
    printf("\nSELECT range of keys\n");
    start = clock();

    sbitsIterator itSelectKeyRange;
    uint32_t minKeySelectKeyRange = minKey + (maxKey - minKey) * 0.15;
    uint32_t maxKeySelectKeyRange = minKey + (maxKey - minKey) * 0.85;
    itSelectKeyRange.minKey = &minKeySelectKeyRange;
    itSelectKeyRange.maxKey = &maxKeySelectKeyRange;
    itSelectKeyRange.minData = NULL;
    itSelectKeyRange.maxData = NULL;
    sbitsInitIterator(state, &itSelectKeyRange);

    int numRecordsSelectKeyRange = 0;
    int numReadsSelectKeyRange = state->numReads;

    while (sbitsNext(state, &itSelectKeyRange, recordBuffer, recordBuffer + state->keySize)) {
        numRecordsSelectKeyRange++;
    }

    clock_t timeSelectKeyRange = (clock() - start) / (CLOCKS_PER_SEC / 1000);

    numReadsSelectKeyRange = state->numReads - numReadsSelectKeyRange;

    printf("Result size: %d\n", numRecordsSelectKeyRange);
    printf("Time: %dms\n", timeSelectKeyRange);
    printf("Num reads: %d\n", numReadsSelectKeyRange);

    //////////////////////////////////
    // SELECT by data, small result //
    //////////////////////////////////
    printf("\nSELECT by data, small result\n");
    start = clock();

    sbitsIterator itSelectDataSmallResult;
    int32_t minDataSelectDataSmallResult = 700;
    itSelectDataSmallResult.minKey = NULL;
    itSelectDataSmallResult.maxKey = NULL;
    itSelectDataSmallResult.minData = &minDataSelectDataSmallResult;
    itSelectDataSmallResult.maxData = NULL;
    sbitsInitIterator(state, &itSelectDataSmallResult);

    int numRecordsSelectDataSmallResult = 0;
    int numReadsSelectDataSmallResult = state->numReads;
    int numIdxReadsSelectDataSmallResult = state->numIdxReads;

    while (sbitsNext(state, &itSelectDataSmallResult, recordBuffer, recordBuffer + state->keySize)) {
        numRecordsSelectDataSmallResult++;
    }

    clock_t timeSelectDataSmallResult = (clock() - start) / (CLOCKS_PER_SEC / 1000);

    numReadsSelectDataSmallResult = state->numReads - numReadsSelectDataSmallResult;
    numIdxReadsSelectDataSmallResult = state->numIdxReads - numIdxReadsSelectDataSmallResult;

    printf("Result size: %d\n", numRecordsSelectDataSmallResult);
    printf("Time: %dms\n", timeSelectDataSmallResult);
    printf("Num reads: %d\n", numReadsSelectDataSmallResult);
    printf("Num idx reads: %d\n", numIdxReadsSelectDataSmallResult);

    //////////////////////////////////
    // SELECT by data, large result //
    //////////////////////////////////
    printf("\nSELECT by data, large result\n");
    start = clock();

    sbitsIterator itSelectDataLargeResult;
    int32_t minDataSelectDataLargeResult = 420;
    itSelectDataLargeResult.minKey = NULL;
    itSelectDataLargeResult.maxKey = NULL;
    itSelectDataLargeResult.minData = &minDataSelectDataLargeResult;
    itSelectDataLargeResult.maxData = NULL;
    sbitsInitIterator(state, &itSelectDataLargeResult);

    int numRecordsSelectDataLargeResult = 0;
    int numReadsSelectDataLargeResult = state->numReads;
    int numIdxReadsSelectDataLargeResult = state->numIdxReads;

    while (sbitsNext(state, &itSelectDataLargeResult, recordBuffer, recordBuffer + state->keySize)) {
        numRecordsSelectDataLargeResult++;
    }

    clock_t timeSelectDataLargeResult = (clock() - start) / (CLOCKS_PER_SEC / 1000);

    numReadsSelectDataLargeResult = state->numReads - numReadsSelectDataLargeResult;
    numIdxReadsSelectDataLargeResult = state->numIdxReads - numIdxReadsSelectDataLargeResult;

    printf("Result size: %d\n", numRecordsSelectDataLargeResult);
    printf("Time: %dms\n", timeSelectDataLargeResult);
    printf("Num reads: %d\n", numReadsSelectDataLargeResult);
    printf("Num idx reads: %d\n", numIdxReadsSelectDataLargeResult);

    ///////////////////////
    // SELECT data range //
    ///////////////////////
    printf("\nSELECT data range\n");
    start = clock();

    sbitsIterator itSelectDataRange;
    int32_t minDataSelectDataRange = 420, maxDataSelectDataRange = 620;
    itSelectDataRange.minKey = NULL;
    itSelectDataRange.maxKey = NULL;
    itSelectDataRange.minData = &minDataSelectDataRange;
    itSelectDataRange.maxData = &maxDataSelectDataRange;
    sbitsInitIterator(state, &itSelectDataRange);

    int numRecordsSelectDataRange = 0;
    int numReadsSelectDataRange = state->numReads;
    int numIdxReadsSelectDataRange = state->numIdxReads;

    while (sbitsNext(state, &itSelectDataRange, recordBuffer, recordBuffer + state->keySize)) {
        numRecordsSelectDataRange++;
    }

    clock_t timeSelectDataRange = (clock() - start) / (CLOCKS_PER_SEC / 1000);

    numReadsSelectDataRange = state->numReads - numReadsSelectDataRange;
    numIdxReadsSelectDataRange = state->numIdxReads - numIdxReadsSelectDataRange;

    printf("Result size: %d\n", numRecordsSelectDataRange);
    printf("Time: %dms\n", timeSelectDataRange);
    printf("Num reads: %d\n", numReadsSelectDataRange);
    printf("Num idx reads: %d\n", numIdxReadsSelectDataRange);

    /////////////////////////////////
    // SELECT on both key and data //
    /////////////////////////////////
    printf("\nSELECT on both key and data\n");
    start = clock();

    sbitsIterator itSelectKeyData;
    uint32_t minKeySelectKeyData = minKey + (maxKey - minKey) * 0.4;
    int32_t minDataSelectKeyData = 450, maxDataSelectKeyData = 650;
    itSelectKeyData.minKey = &minKeySelectKeyData;
    itSelectKeyData.maxKey = NULL;
    itSelectKeyData.minData = &minDataSelectKeyData;
    itSelectKeyData.maxData = &maxDataSelectKeyData;
    sbitsInitIterator(state, &itSelectKeyData);

    int numRecordsSelectKeyData = 0;
    int numReadsSelectKeyData = state->numReads;
    int numIdxReadsSelectKeyData = state->numIdxReads;

    while (sbitsNext(state, &itSelectKeyData, recordBuffer, recordBuffer + state->keySize)) {
        numRecordsSelectKeyData++;
    }

    clock_t timeSelectKeyData = (clock() - start) / (CLOCKS_PER_SEC / 1000);

    numReadsSelectKeyData = state->numReads - numReadsSelectKeyData;
    numIdxReadsSelectKeyData = state->numIdxReads - numIdxReadsSelectKeyData;

    printf("Result size: %d\n", numRecordsSelectKeyData);
    printf("Time: %dms\n", timeSelectKeyData);
    printf("Num reads: %d\n", numReadsSelectKeyData);
    printf("Num idx reads: %d\n", numIdxReadsSelectKeyData);

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

    /*  Custom, equi-depth buckets
     *  Bucket  0: 303 -> 391 (0, 31250)
     *	Bucket  1: 391 -> 418 (31251, 62502)
     *	Bucket  2: 418 -> 436 (62503, 93754)
     *	Bucket  3: 436 -> 454 (93755, 125006)
     *	Bucket  4: 454 -> 469 (125007, 156258)
     *	Bucket  5: 469 -> 484 (156259, 187510)
     *	Bucket  6: 484 -> 502 (187511, 218762)
     *	Bucket  7: 502 -> 522 (218763, 250014)
     *	Bucket  8: 522 -> 542 (250015, 281265)
     *	Bucket  9: 542 -> 560 (281266, 312517)
     *	Bucket 10: 560 -> 577 (312518, 343769)
     *	Bucket 11: 577 -> 594 (343770, 375021)
     *	Bucket 12: 594 -> 615 (375022, 406273)
     *	Bucket 13: 615 -> 642 (406274, 437525)
     *	Bucket 14: 642 -> 685 (437526, 468777)
     *	Bucket 15: 685 -> 892 (468778, 500029)
     */

    uint16_t mask = 1;

    int8_t mode = 0;  // 0 = equi-depth, 1 = equi-width
    if (mode == 0) {
        if (temp < 391) {
            mask <<= 0;
        } else if (temp < 418) {
            mask <<= 1;
        } else if (temp < 436) {
            mask <<= 2;
        } else if (temp < 454) {
            mask <<= 3;
        } else if (temp < 469) {
            mask <<= 4;
        } else if (temp < 484) {
            mask <<= 5;
        } else if (temp < 502) {
            mask <<= 6;
        } else if (temp < 522) {
            mask <<= 7;
        } else if (temp < 542) {
            mask <<= 8;
        } else if (temp < 560) {
            mask <<= 9;
        } else if (temp < 577) {
            mask <<= 10;
        } else if (temp < 594) {
            mask <<= 11;
        } else if (temp < 615) {
            mask <<= 12;
        } else if (temp < 642) {
            mask <<= 13;
        } else if (temp < 685) {
            mask <<= 14;
        } else {
            mask <<= 15;
        }
    } else {
        int shift = (temp - 303) / 16;
        if (shift < 0) {
            shift = 0;
        } else if (shift > 15) {
            shift = 15;
        }
        mask <<= shift;
    }

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
            minMap = ~(minMap - 1);
            if (max == NULL) {
                *(uint16_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateCustomUWABitmap(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = maxMap | (maxMap - 1);
            if (min == NULL) {
                *(uint16_t *)bm = maxMap;
                return;
            }
        }
        *(uint16_t *)bm = minMap & maxMap;
    }
}
