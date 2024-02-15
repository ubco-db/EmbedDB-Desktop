// Compiles with: gcc embedDBBenchmarkEthylene4M.c ../src/embedDB/embedDB.c ../src/embedDB/utilityFunctions.c ../src/spline/spline.c ../src/spline/radixspline.c -o embedDBBenchmarkEthylene4M

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../src/embedDB/embedDB.h"
#include "../src/embedDB/utilityFunctions.h"

void updateCustomEthBitmap(void *data, void *bm);
int8_t inCustomEthBitmap(void *data, void *bm);
void buildCustomEthBitmapFromRange(void *min, void *max, void *bm);
int8_t customCol3Int32Comparator(void *a, void *b);

char const dataFileName[] = "../data/ethylene_CO_only_4M.bin";
char const randomizedDataFileName[] = "../data/ethylene_CO_only_4M_randomized.bin";
#define numRuns 10

int main() {
    double timeInsert[numRuns],
        timeSelectAll[numRuns],
        timeSelectKeySmallResult[numRuns],
        timeSelectKeyLargeResult[numRuns],
        timeSelectSingleDataResult[numRuns],
        timeSelectDataSmallResult[numRuns],
        timeSelectDataLargeResult[numRuns],
        timeSelectKeyData[numRuns],
        timeSeqKV[numRuns],
        timeRandKV[numRuns];
    uint32_t numRecords, numRecordsSelectAll, numRecordsSelectKeySmallResult, numRecordsSelectKeyLargeResult, numRecordsSelectSingleDataResult, numRecordsSelectDataSmallResult, numRecordsSelectDataLargeResult, numRecordsSelectKeyData, numRecordsSeqKV, numRecordsRandKV;
    uint32_t numWrites, numReadsSelectAll, numReadsSelectKeySmallResult, numReadsSelectKeyLargeResult, numReadsSelectSingleDataResult, numReadsSelectDataSmallResult, numReadsSelectDataLargeResult, numReadsSelectKeyData, numReadsSeqKV, numReadsRandKV;
    uint32_t numIdxWrites, numIdxReadsSelectSingleDataResult, numIdxReadsSelectDataSmallResult, numIdxReadsSelectDataLargeResult, numIdxReadsSelectKeyData;

    for (int run = 0; run < numRuns; run++) {
        ///////////
        // Setup //
        ///////////
        embedDBState *state = malloc(sizeof(embedDBState));
        state->keySize = 4;
        state->dataSize = 12;
        state->compareKey = int32Comparator;
        state->compareData = customCol3Int32Comparator;
        state->pageSize = 512;
        state->eraseSizeInPages = 4;
        state->numSplinePoints = 3000;
        state->numDataPages = 200000;
        state->numIndexPages = 1000;
        state->fileInterface = getFileInterface();
        char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
        state->dataFile = setupFile(dataPath);
        state->indexFile = setupFile(indexPath);
        state->bufferSizeInBlocks = 4;
        state->buffer = calloc(state->bufferSizeInBlocks, state->pageSize);
        state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
        state->bitmapSize = 2;
        state->inBitmap = inCustomEthBitmap;
        state->updateBitmap = updateCustomEthBitmap;
        state->buildBitmapFromRange = buildCustomEthBitmapFromRange;
        embedDBInit(state, 1);

        char *recordBuffer = malloc(state->recordSize);

        ////////////////////////
        // Insert uwa dataset //
        ////////////////////////
        FILE *dataset = fopen(dataFileName, "rb");
        if (dataset == NULL) {
            printf("Error: Could not open file %s\n", dataFileName);
            return 1;
        }

        double start = (double)clock();

        numRecords = 0;
        uint32_t minKey = 0;
        uint32_t maxKey = 109947;

        char dataPage[512];
        while (fread(dataPage, 512, 1, dataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                embedDBPut(state, dataPage + record * state->recordSize, dataPage + record * state->recordSize + state->keySize);
                numRecords++;
            }
        }
        embedDBFlush(state);

        timeInsert[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000.0);
        numWrites = state->numWrites;
        numIdxWrites = state->numIdxWrites;

        /////////////////////
        // SELECT * FROM r //
        /////////////////////
        start = (double)clock();

        embedDBIterator itSelectAll;
        itSelectAll.minKey = NULL;
        itSelectAll.maxKey = NULL;
        itSelectAll.minData = NULL;
        itSelectAll.maxData = NULL;
        embedDBInitIterator(state, &itSelectAll);

        numRecordsSelectAll = 0;
        numReadsSelectAll = state->numReads;

        while (embedDBNext(state, &itSelectAll, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectAll++;
        }

        timeSelectAll[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000.0);

        numReadsSelectAll = state->numReads - numReadsSelectAll;

        ///////////////
        // SELECT 5% //
        ///////////////
        start = (double)clock();

        embedDBIterator itSelectKeySmallResult;
        uint32_t minKeySelectKeySmallResult = 3923162;
        itSelectKeySmallResult.minKey = &minKeySelectKeySmallResult;
        itSelectKeySmallResult.maxKey = NULL;
        itSelectKeySmallResult.minData = NULL;
        itSelectKeySmallResult.maxData = NULL;
        embedDBInitIterator(state, &itSelectKeySmallResult);

        numRecordsSelectKeySmallResult = 0;
        numReadsSelectKeySmallResult = state->numReads;

        while (embedDBNext(state, &itSelectKeySmallResult, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectKeySmallResult++;
        }

        timeSelectKeySmallResult[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000.0);

        numReadsSelectKeySmallResult = state->numReads - numReadsSelectKeySmallResult;

        ////////////////
        // SELECT 80% //
        ////////////////
        start = (double)clock();

        embedDBIterator itSelectKeyLargeResult;
        uint32_t minKeySelectKeyLargeResult = 832175;
        itSelectKeyLargeResult.minKey = &minKeySelectKeyLargeResult;
        itSelectKeyLargeResult.maxKey = NULL;
        itSelectKeyLargeResult.minData = NULL;
        itSelectKeyLargeResult.maxData = NULL;
        embedDBInitIterator(state, &itSelectKeyLargeResult);

        numRecordsSelectKeyLargeResult = 0;
        numReadsSelectKeyLargeResult = state->numReads;

        while (embedDBNext(state, &itSelectKeyLargeResult, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectKeyLargeResult++;
        }

        timeSelectKeyLargeResult[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000.0);

        numReadsSelectKeyLargeResult = state->numReads - numReadsSelectKeyLargeResult;

        /////////////////////////////////////////
        // SELECT * FROM r WHERE data = 214517 //
        /////////////////////////////////////////

        start = (double)clock();

        embedDBIterator itSelectSingleDataResult;
        int32_t minDataSelectSingleResult[] = {0, 0, 214517};
        int32_t maxDataSelectSingleResult[] = {0, 0, 214517};
        itSelectSingleDataResult.minKey = NULL;
        itSelectSingleDataResult.maxKey = NULL;
        itSelectSingleDataResult.minData = minDataSelectSingleResult;
        itSelectSingleDataResult.maxData = maxDataSelectSingleResult;
        embedDBInitIterator(state, &itSelectSingleDataResult);

        numRecordsSelectSingleDataResult = 0;
        numReadsSelectSingleDataResult = state->numReads;
        numIdxReadsSelectSingleDataResult = state->numIdxReads;

        while (embedDBNext(state, &itSelectSingleDataResult, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectSingleDataResult++;
        }

        timeSelectSingleDataResult[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000.0);

        numReadsSelectSingleDataResult = state->numReads - numReadsSelectSingleDataResult;
        numIdxReadsSelectSingleDataResult = state->numIdxReads - numIdxReadsSelectSingleDataResult;

        //////////////////////////////////////////
        // SELECT * FROM r WHERE data >= 350000 //
        //////////////////////////////////////////
        start = (double)clock();

        embedDBIterator itSelectDataSmallResult;
        int32_t minDataSelectDataSmallResult[] = {0, 0, 350000};
        itSelectDataSmallResult.minKey = NULL;
        itSelectDataSmallResult.maxKey = NULL;
        itSelectDataSmallResult.minData = minDataSelectDataSmallResult;
        itSelectDataSmallResult.maxData = NULL;
        embedDBInitIterator(state, &itSelectDataSmallResult);

        numRecordsSelectDataSmallResult = 0;
        numReadsSelectDataSmallResult = state->numReads;
        numIdxReadsSelectDataSmallResult = state->numIdxReads;

        while (embedDBNext(state, &itSelectDataSmallResult, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectDataSmallResult++;
        }

        timeSelectDataSmallResult[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000.0);

        numReadsSelectDataSmallResult = state->numReads - numReadsSelectDataSmallResult;
        numIdxReadsSelectDataSmallResult = state->numIdxReads - numIdxReadsSelectDataSmallResult;

        //////////////////////////////////////////
        // SELECT * FROM r WHERE data >= 149000 //
        //////////////////////////////////////////
        start = (double)clock();

        embedDBIterator itSelectDataLargeResult;
        int32_t minDataSelectDataLargeResult[] = {0, 0, 149000};
        itSelectDataLargeResult.minKey = NULL;
        itSelectDataLargeResult.maxKey = NULL;
        itSelectDataLargeResult.minData = minDataSelectDataLargeResult;
        itSelectDataLargeResult.maxData = NULL;
        embedDBInitIterator(state, &itSelectDataLargeResult);

        numRecordsSelectDataLargeResult = 0;
        numReadsSelectDataLargeResult = state->numReads;
        numIdxReadsSelectDataLargeResult = state->numIdxReads;

        while (embedDBNext(state, &itSelectDataLargeResult, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectDataLargeResult++;
        }

        timeSelectDataLargeResult[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000.0);

        numReadsSelectDataLargeResult = state->numReads - numReadsSelectDataLargeResult;
        numIdxReadsSelectDataLargeResult = state->numIdxReads - numIdxReadsSelectDataLargeResult;

        // //////////////////////////////////////////////////////////////////////////////
        // // SELECT * FROM r WHERE key >= 43978 AND data >= 149000 AND data <= 215000 //
        // //////////////////////////////////////////////////////////////////////////////
        // start = (double)clock();

        // embedDBIterator itSelectKeyData;
        // uint32_t minKeySelectKeyData = minKey + (maxKey - minKey) * 0.4;  // 43978
        // int32_t minDataSelectKeyData[] = {0, 0, 149000}, maxDataSelectKeyData[] = {0, 0, 215000};
        // itSelectKeyData.minKey = &minKeySelectKeyData;
        // itSelectKeyData.maxKey = NULL;
        // itSelectKeyData.minData = minDataSelectKeyData;
        // itSelectKeyData.maxData = maxDataSelectKeyData;
        // embedDBInitIterator(state, &itSelectKeyData);

        // numRecordsSelectKeyData = 0;
        // numReadsSelectKeyData = state->numReads;
        // numIdxReadsSelectKeyData = state->numIdxReads;

        // while (embedDBNext(state, &itSelectKeyData, recordBuffer, recordBuffer + state->keySize)) {
        //     numRecordsSelectKeyData++;
        // }

        // timeSelectKeyData[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000.0);

        // numReadsSelectKeyData = state->numReads - numReadsSelectKeyData;
        // numIdxReadsSelectKeyData = state->numIdxReads - numIdxReadsSelectKeyData;

        //////////////////////////
        // Sequential Key-Value //
        //////////////////////////
        fseek(dataset, 0, SEEK_SET);
        start = (double)clock();

        numRecordsSeqKV = 0;
        numReadsSeqKV = state->numReads;

        while (fread(dataPage, 512, 1, dataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                embedDBGet(state, dataPage + record * state->recordSize, recordBuffer);
                numRecordsSeqKV++;
            }
        }

        timeSeqKV[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000.0);
        numReadsSeqKV = state->numReads - numReadsSeqKV;

        //////////////////////
        // Random Key-Value //
        //////////////////////
        FILE *randomDataset = fopen(randomizedDataFileName, "rb");

        start = (double)clock();

        numRecordsRandKV = 0;
        numReadsRandKV = state->numReads;

        while (fread(dataPage, 512, 1, randomDataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                embedDBGet(state, dataPage + record * state->recordSize, recordBuffer);
                numRecordsRandKV++;
            }
        }

        timeRandKV[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000.0);
        numReadsRandKV = state->numReads - numReadsRandKV;

        fclose(dataset);
        fclose(randomDataset);

        ///////////////////
        // Close EmbedDB //
        ///////////////////
        embedDBClose(state);
        tearDownFile(state->dataFile);
        tearDownFile(state->indexFile);
        free(state->fileInterface);
        free(state->buffer);
        free(recordBuffer);
        free(state);
    }

    // Calculate averages
    double sum = 0;
    printf("\nINSERT\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%.1f ", timeInsert[i]);
        sum += timeInsert[i];
    }
    printf("~ %fms\n", sum / numRuns);
    printf("Num Records inserted: %d\n", numRecords);
    printf("Num data pages written: %d\n", numWrites);
    printf("Num index pages written: %d\n", numIdxWrites);

    sum = 0;
    printf("\nSELECT * FROM r\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%.1f ", timeSelectAll[i]);
        sum += timeSelectAll[i];
    }
    printf("~ %fms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSelectAll);
    printf("Num reads: %d\n", numReadsSelectAll);

    sum = 0;
    printf("\nSELECT Continuous 5%% (key >= 3923162)\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%.1f ", timeSelectKeySmallResult[i]);
        sum += timeSelectKeySmallResult[i];
    }
    printf("~ %fms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSelectKeySmallResult);
    printf("Num reads: %d\n", numReadsSelectKeySmallResult);

    sum = 0;
    printf("\nSELECT Continuous 80%% (key >= 832175)\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%.1f ", timeSelectKeyLargeResult[i]);
        sum += timeSelectKeyLargeResult[i];
    }
    printf("~ %fms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSelectKeyLargeResult);
    printf("Num reads: %d\n", numReadsSelectKeyLargeResult);

    sum = 0;
    printf("\nSELECT * FROM r WHERE data = 214517\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%.1f ", timeSelectSingleDataResult[i]);
        sum += timeSelectSingleDataResult[i];
    }
    printf("~ %fms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSelectSingleDataResult);
    printf("Num reads: %d\n", numReadsSelectSingleDataResult);
    printf("Num idx reads: %d\n", numIdxReadsSelectSingleDataResult);

    sum = 0;
    printf("\nSELECT * FROM r WHERE data >= 350000\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%.1f ", timeSelectDataSmallResult[i]);
        sum += timeSelectDataSmallResult[i];
    }
    printf("~ %fms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSelectDataSmallResult);
    printf("Num reads: %d\n", numReadsSelectDataSmallResult);
    printf("Num idx reads: %d\n", numIdxReadsSelectDataSmallResult);

    sum = 0;
    printf("\nSELECT * FROM r WHERE data >= 149000\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%.1f ", timeSelectDataLargeResult[i]);
        sum += timeSelectDataLargeResult[i];
    }
    printf("~ %fms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSelectDataLargeResult);
    printf("Num reads: %d\n", numReadsSelectDataLargeResult);
    printf("Num idx reads: %d\n", numIdxReadsSelectDataLargeResult);

    // sum = 0;
    // printf("\nSELECT * FROM r WHERE key >= 43978 AND data >= 149000 AND data <= 215000\n");
    // printf("Time: ");
    // for (int i = 0; i < numRuns; i++) {
    //     printf("%.1f ", timeSelectKeyData[i]);
    //     sum += timeSelectKeyData[i];
    // }
    // printf("~ %fms\n", sum / numRuns);
    // printf("Result size: %d\n", numRecordsSelectKeyData);
    // printf("Num reads: %d\n", numReadsSelectKeyData);
    // printf("Num idx reads: %d\n", numIdxReadsSelectKeyData);

    sum = 0;
    printf("\nSequential Key-Value\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%.1f ", timeSeqKV[i]);
        sum += timeSeqKV[i];
    }
    printf("~ %fms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSeqKV);
    printf("Num reads: %d\n", numReadsSeqKV);

    sum = 0;
    printf("\nRandom Key-Value\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%.1f ", timeRandKV[i]);
        sum += timeRandKV[i];
    }
    printf("~ %fms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsRandKV);
    printf("Num reads: %d\n", numReadsRandKV);

    return 0;
}

void updateCustomEthBitmap(void *data, void *bm) {
    int32_t temp = *(int32_t *)((int8_t *)data + 8);

    /* Custom, equi-depth buckets
     * Bucket  0: -5230 -> 113306 (0, 249999)
     * Bucket  1: 113306 -> 119146 (250000, 499999)
     * Bucket  2: 119146 -> 125234 (500000, 749999)
     * Bucket  3: 125234 -> 132618 (750000, 999999)
     * Bucket  4: 132618 -> 141265 (1000000, 1249999)
     * Bucket  5: 141265 -> 149636 (1250000, 1499999)
     * Bucket  6: 149636 -> 160888 (1500000, 1749999)
     * Bucket  7: 160888 -> 178773 (1750000, 1999999)
     * Bucket  8: 178773 -> 215838 (2000000, 2249999)
     * Bucket  9: 215838 -> 250127 (2250000, 2499999)
     * Bucket 10: 250127 -> 274266 (2500000, 2749999)
     * Bucket 11: 274266 -> 287160 (2750000, 2999999)
     * Bucket 12: 287160 -> 300703 (3000000, 3249999)
     * Bucket 13: 300703 -> 312849 (3250000, 3499999)
     * Bucket 14: 312849 -> 324452 (3500000, 3749999)
     * Bucket 15: 324452 -> 355364 (3750000, 3999999)
     */

    uint16_t mask = 1;

    if (temp < 113306) {
        mask <<= 0;
    } else if (temp < 119146) {
        mask <<= 1;
    } else if (temp < 125234) {
        mask <<= 2;
    } else if (temp < 132618) {
        mask <<= 3;
    } else if (temp < 141265) {
        mask <<= 4;
    } else if (temp < 149636) {
        mask <<= 5;
    } else if (temp < 160888) {
        mask <<= 6;
    } else if (temp < 178773) {
        mask <<= 7;
    } else if (temp < 215838) {
        mask <<= 8;
    } else if (temp < 250127) {
        mask <<= 9;
    } else if (temp < 274266) {
        mask <<= 10;
    } else if (temp < 287160) {
        mask <<= 11;
    } else if (temp < 300703) {
        mask <<= 12;
    } else if (temp < 312849) {
        mask <<= 13;
    } else if (temp < 324452) {
        mask <<= 14;
    } else {
        mask <<= 15;
    }

    *(uint16_t *)bm |= mask;
}

int8_t inCustomEthBitmap(void *data, void *bm) {
    uint16_t *bmval = (uint16_t *)bm;

    uint16_t tmpbm = 0;
    updateCustomEthBitmap(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return (tmpbm & *bmval) > 0;
}

void buildCustomEthBitmapFromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint16_t *)bm = 65535; /* Everything */
        return;
    } else {
        uint16_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateCustomEthBitmap(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = ~(minMap - 1);
            if (max == NULL) {
                *(uint16_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateCustomEthBitmap(max, &maxMap);
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

int8_t customCol3Int32Comparator(void *a, void *b) {
    int32_t i1, i2;
    memcpy(&i1, (int8_t *)a + 8, sizeof(int32_t));
    memcpy(&i2, (int8_t *)b + 8, sizeof(int32_t));
    int32_t result = i1 - i2;
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}
