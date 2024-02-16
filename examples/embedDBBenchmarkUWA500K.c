#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../src/embedDB/embedDB.h"
#include "../src/embedDB/utilityFunctions.h"

void updateCustomUWABitmap(void *data, void *bm);
int8_t inCustomUWABitmap(void *data, void *bm);
void buildCustomUWABitmapFromRange(void *min, void *max, void *bm);

char const dataFileName[] = "../data/uwa500K.bin";
char const randomizedDataFileName[] = "../data/uwa500K_randomized.bin";
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
        state->compareData = int32Comparator;
        state->pageSize = 512;
        state->eraseSizeInPages = 4;
        state->numSplinePoints = 300;
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
        state->inBitmap = inCustomUWABitmap;
        state->updateBitmap = updateCustomUWABitmap;
        state->buildBitmapFromRange = buildCustomUWABitmapFromRange;
        embedDBInit(state, 1);

        char *recordBuffer = malloc(state->recordSize);

        ////////////////////////
        // Insert uwa dataset //
        ////////////////////////
        FILE *dataset = fopen(dataFileName, "rb");
        double start = (double)clock();

        numRecords = 0;

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
        uint32_t minKeySelectKeySmallResult = 975643380;
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
        uint32_t minKeySelectKeyLargeResult = 952726380;
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

        //////////////////////////////////////
        // SELECT * FROM r WHERE data = 859 //
        //////////////////////////////////////

        start = (double)clock();

        embedDBIterator itSelectSingleDataResult;
        int32_t minDataSelectSingleResult = 850;
        int32_t maxDataSelectSingleResult = 850;
        itSelectSingleDataResult.minKey = NULL;
        itSelectSingleDataResult.maxKey = NULL;
        itSelectSingleDataResult.minData = &minDataSelectSingleResult;
        itSelectSingleDataResult.maxData = &maxDataSelectSingleResult;
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

        ///////////////////////////////////////
        // SELECT * FROM r WHERE data >= 850 //
        ///////////////////////////////////////
        start = (double)clock();

        embedDBIterator itSelectDataSmallResult;
        int32_t minDataSelectDataSmallResult = 850;
        itSelectDataSmallResult.minKey = NULL;
        itSelectDataSmallResult.maxKey = NULL;
        itSelectDataSmallResult.minData = &minDataSelectDataSmallResult;
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

        ///////////////////////////////////////
        // SELECT * FROM r WHERE data >= 480 //
        ///////////////////////////////////////
        start = (double)clock();

        embedDBIterator itSelectDataLargeResult;
        int32_t minDataSelectDataLargeResult = 480;
        itSelectDataLargeResult.minKey = NULL;
        itSelectDataLargeResult.maxKey = NULL;
        itSelectDataLargeResult.minData = &minDataSelectDataLargeResult;
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

        // ////////////////////////////////////////////////////////////////////////////
        // // SELECT * FROM r WHERE key >= 949118688 AND data >= 450 AND data <= 650 //
        // ////////////////////////////////////////////////////////////////////////////
        // start = (double)clock();

        // embedDBIterator itSelectKeyData;
        // uint32_t minKeySelectKeyData = minKey + (maxKey - minKey) * 0.4;  // 949118688
        // int32_t minDataSelectKeyData = 450, maxDataSelectKeyData = 650;
        // itSelectKeyData.minKey = &minKeySelectKeyData;
        // itSelectKeyData.maxKey = NULL;
        // itSelectKeyData.minData = &minDataSelectKeyData;
        // itSelectKeyData.maxData = &maxDataSelectKeyData;
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

        fclose(dataset);

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

        fclose(randomDataset);

        /////////////////
        // Close EMBEDDB //
        /////////////////
        embedDBClose(state);
        tearDownFile(state->dataFile);
        tearDownFile(state->indexFile);
        free(state->fileInterface);
        free(state->buffer);
        free(recordBuffer);
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
    printf("\nSELECT Continuous 5%% (key >= 975643380)\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%.1f ", timeSelectKeySmallResult[i]);
        sum += timeSelectKeySmallResult[i];
    }
    printf("~ %fms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSelectKeySmallResult);
    printf("Num reads: %d\n", numReadsSelectKeySmallResult);

    sum = 0;
    printf("\nSELECT Continuous 80%% (key >= 952726380)\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%.1f ", timeSelectKeyLargeResult[i]);
        sum += timeSelectKeyLargeResult[i];
    }
    printf("~ %fms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSelectKeyLargeResult);
    printf("Num reads: %d\n", numReadsSelectKeyLargeResult);

    sum = 0;
    printf("\nSELECT * FROM r WHERE data = 850\n");
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
    printf("\nSELECT * FROM r WHERE data >= 850\n");
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
    printf("\nSELECT * FROM r WHERE data >= 480\n");
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
    // printf("\nSELECT * FROM r WHERE key >= 949118688 AND data >= 450 AND data <= 650\n");
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

void updateCustomUWABitmap(void *data, void *bm) {
    int32_t temp = *(int32_t *)data;

    /*  Custom, equi-depth buckets
     *  Bucket  0: 303 -> 391 (0, 31249)
     *	Bucket  1: 391 -> 418 (31250, 62499)
     *	Bucket  2: 418 -> 436 (62500, 93749)
     *	Bucket  3: 436 -> 454 (93750, 124999)
     *	Bucket  4: 454 -> 469 (125000, 156249)
     *	Bucket  5: 469 -> 484 (156250, 187499)
     *	Bucket  6: 484 -> 502 (187500, 218749)
     *	Bucket  7: 502 -> 522 (218750, 249999)
     *	Bucket  8: 522 -> 542 (250000, 281249)
     *	Bucket  9: 542 -> 560 (281250, 312499)
     *	Bucket 10: 560 -> 577 (312500, 343749)
     *	Bucket 11: 577 -> 594 (343750, 374999)
     *	Bucket 12: 594 -> 615 (375000, 406249)
     *	Bucket 13: 615 -> 642 (406250, 437499)
     *	Bucket 14: 642 -> 685 (437500, 468749)
     *	Bucket 15: 685 -> 892 (468750, 499999)
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
