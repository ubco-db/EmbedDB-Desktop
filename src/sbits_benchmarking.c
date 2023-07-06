#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "sbits/sbits.h"
#include "sbits/utilityFunctions.h"

void updateCustomUWABitmap(void *data, void *bm);
int8_t inCustomUWABitmap(void *data, void *bm);
void buildCustomUWABitmapFromRange(void *min, void *max, void *bm);

void testRawPerformance() { /* Tests storage raw read and write performance */
    printf("Starting RAW performance test.\n");

    char buffer[512];
    clock_t start;
    /* SD Card */
    FILE *fp = fopen("build/artifacts/myfile.bin", "w+b");
    if (fp == NULL) {
        printf("Error opening file.\n");
        return;
    }

    // Test time to write 100000 blocks
    int numWrites = 100000;
    start = clock();

    for (int i = 0; i < numWrites; i++) {
        if (0 == fwrite(buffer, 512, 1, fp)) {
            printf("Write error.\n");
        }
    }
    printf("Write time: %lums (%.2f MB/s)\n", (clock() - start) / (CLOCKS_PER_SEC / 1000), (double)numWrites * 512 / 1000000 / ((clock() - start) / (CLOCKS_PER_SEC / 1000)) * 1000);
    fflush(fp);

    start = clock();

    for (int i = 0; i < numWrites; i++) {
        unsigned long num = rand() % numWrites;
        fseek(fp, num * 512, SEEK_SET);
        if (0 == fwrite(buffer, 512, 1, fp)) {
            printf("Write error.\n");
        }
    }
    printf("Random write time: %lums (%.2f MB/s)\n", (clock() - start) / (CLOCKS_PER_SEC / 1000), (double)numWrites * 512 / 1000000 / ((clock() - start) / (CLOCKS_PER_SEC / 1000)) * 1000);
    fflush(fp);

    // Time to read 1000 blocks
    fseek(fp, 0, SEEK_SET);
    start = clock();
    for (int i = 0; i < numWrites; i++) {
        if (0 == fread(buffer, 512, 1, fp)) {
            printf("Read error.\n");
        }
    }
    printf("Read time: %lums (%.2f MB/s)\n", (clock() - start) / (CLOCKS_PER_SEC / 1000), (double)numWrites * 512 / 1000000 / ((clock() - start) / (CLOCKS_PER_SEC / 1000)) * 1000);

    fseek(fp, 0, SEEK_SET);
    // Time to read 1000 blocks randomly
    start = clock();
    srand(1);
    for (int i = 0; i < numWrites; i++) {
        unsigned long num = rand() % numWrites;
        fseek(fp, num * 512, SEEK_SET);
        if (0 == fread(buffer, 512, 1, fp)) {
            printf("Read error.\n");
        }
    }
    printf("Random Read time: %lums (%.2f MB/s)\n", (clock() - start) / (CLOCKS_PER_SEC / 1000), (double)numWrites * 512 / 1000000 / ((clock() - start) / (CLOCKS_PER_SEC / 1000)) * 1000);
}

int main() {
    printf("\n");
    testRawPerformance();
    printf("\n");

    int numRuns = 50;
    clock_t timeInsert[numRuns],
        timeSelectAll[numRuns],
        timeSelectKeySmallResult[numRuns],
        timeSelectKeyLargeResult[numRuns],
        timeSelectDataSmallResult[numRuns],
        timeSelectDataLargeResult[numRuns],
        timeSelectKeyData[numRuns],
        timeSeqKV[numRuns],
        timeRandKV[numRuns];
    uint32_t numRecords, numRecordsSelectAll, numRecordsSelectKeySmallResult, numRecordsSelectKeyLargeResult, numRecordsSelectDataSmallResult, numRecordsSelectDataLargeResult, numRecordsSelectKeyData, numRecordsSeqKV, numRecordsRandKV;
    uint32_t numWrites, numReadsSelectAll, numReadsSelectKeySmallResult, numReadsSelectKeyLargeResult, numReadsSelectDataSmallResult, numReadsSelectDataLargeResult, numReadsSelectKeyData, numReadsSeqKV, numReadsRandKV;
    uint32_t numIdxWrites, numIdxReadsSelectDataSmallResult, numIdxReadsSelectDataLargeResult, numIdxReadsSelectKeyData;

    for (int run = 0; run < numRuns; run++) {
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
        FILE *dataset = fopen("data/uwa500K.bin", "rb");
        clock_t start = clock();

        numRecords = 0;
        uint32_t minKey = 946713600;
        uint32_t maxKey = 977144040;

        char dataPage[512];
        while (fread(dataPage, 512, 1, dataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                sbitsPut(state, dataPage + record * state->recordSize, dataPage + record * state->recordSize + state->keySize);
                numRecords++;
            }
        }
        sbitsFlush(state);

        timeInsert[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000);
        numWrites = state->numWrites;
        numIdxWrites = state->numIdxWrites;

        /////////////////////
        // SELECT * FROM r //
        /////////////////////
        start = clock();

        sbitsIterator itSelectAll;
        itSelectAll.minKey = NULL;
        itSelectAll.maxKey = NULL;
        itSelectAll.minData = NULL;
        itSelectAll.maxData = NULL;
        sbitsInitIterator(state, &itSelectAll);

        numRecordsSelectAll = 0;
        numReadsSelectAll = state->numReads;

        while (sbitsNext(state, &itSelectAll, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectAll++;
        }

        timeSelectAll[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000);

        numReadsSelectAll = state->numReads - numReadsSelectAll;

        ////////////////////////////////////////////
        // SELECT * FROM r WHERE key >= 974100996 //
        ////////////////////////////////////////////
        start = clock();

        sbitsIterator itSelectKeySmallResult;
        uint32_t minKeySelectKeySmallResult = maxKey - (maxKey - minKey) * 0.1;
        itSelectKeySmallResult.minKey = &minKeySelectKeySmallResult;
        itSelectKeySmallResult.maxKey = NULL;
        itSelectKeySmallResult.minData = NULL;
        itSelectKeySmallResult.maxData = NULL;
        sbitsInitIterator(state, &itSelectKeySmallResult);

        numRecordsSelectKeySmallResult = 0;
        numReadsSelectKeySmallResult = state->numReads;

        while (sbitsNext(state, &itSelectKeySmallResult, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectKeySmallResult++;
        }

        timeSelectKeySmallResult[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000);

        numReadsSelectKeySmallResult = state->numReads - numReadsSelectKeySmallResult;

        ////////////////////////////////////////////
        // SELECT * FROM r WHERE key >= 949756644 //
        ////////////////////////////////////////////
        start = clock();

        sbitsIterator itSelectKeyLargeResult;
        uint32_t minKeySelectKeyLargeResult = maxKey - (maxKey - minKey) * 0.9;
        itSelectKeyLargeResult.minKey = &minKeySelectKeyLargeResult;
        itSelectKeyLargeResult.maxKey = NULL;
        itSelectKeyLargeResult.minData = NULL;
        itSelectKeyLargeResult.maxData = NULL;
        sbitsInitIterator(state, &itSelectKeyLargeResult);

        numRecordsSelectKeyLargeResult = 0;
        numReadsSelectKeyLargeResult = state->numReads;

        while (sbitsNext(state, &itSelectKeyLargeResult, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectKeyLargeResult++;
        }

        timeSelectKeyLargeResult[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000);

        numReadsSelectKeyLargeResult = state->numReads - numReadsSelectKeyLargeResult;

        ///////////////////////////////////////
        // SELECT * FROM r WHERE data >= 700 //
        ///////////////////////////////////////
        start = clock();

        sbitsIterator itSelectDataSmallResult;
        int32_t minDataSelectDataSmallResult = 700;
        itSelectDataSmallResult.minKey = NULL;
        itSelectDataSmallResult.maxKey = NULL;
        itSelectDataSmallResult.minData = &minDataSelectDataSmallResult;
        itSelectDataSmallResult.maxData = NULL;
        sbitsInitIterator(state, &itSelectDataSmallResult);

        numRecordsSelectDataSmallResult = 0;
        numReadsSelectDataSmallResult = state->numReads;
        numIdxReadsSelectDataSmallResult = state->numIdxReads;

        while (sbitsNext(state, &itSelectDataSmallResult, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectDataSmallResult++;
        }

        timeSelectDataSmallResult[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000);

        numReadsSelectDataSmallResult = state->numReads - numReadsSelectDataSmallResult;
        numIdxReadsSelectDataSmallResult = state->numIdxReads - numIdxReadsSelectDataSmallResult;

        ///////////////////////////////////////
        // SELECT * FROM r WHERE data >= 420 //
        ///////////////////////////////////////
        start = clock();

        sbitsIterator itSelectDataLargeResult;
        int32_t minDataSelectDataLargeResult = 420;
        itSelectDataLargeResult.minKey = NULL;
        itSelectDataLargeResult.maxKey = NULL;
        itSelectDataLargeResult.minData = &minDataSelectDataLargeResult;
        itSelectDataLargeResult.maxData = NULL;
        sbitsInitIterator(state, &itSelectDataLargeResult);

        numRecordsSelectDataLargeResult = 0;
        numReadsSelectDataLargeResult = state->numReads;
        numIdxReadsSelectDataLargeResult = state->numIdxReads;

        while (sbitsNext(state, &itSelectDataLargeResult, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectDataLargeResult++;
        }

        timeSelectDataLargeResult[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000);

        numReadsSelectDataLargeResult = state->numReads - numReadsSelectDataLargeResult;
        numIdxReadsSelectDataLargeResult = state->numIdxReads - numIdxReadsSelectDataLargeResult;

        ////////////////////////////////////////////////////////////////////////////
        // SELECT * FROM r WHERE key >= 958885776 AND data >= 450 AND data <= 650 //
        ////////////////////////////////////////////////////////////////////////////
        start = clock();

        sbitsIterator itSelectKeyData;
        uint32_t minKeySelectKeyData = minKey + (maxKey - minKey) * 0.4;
        int32_t minDataSelectKeyData = 450, maxDataSelectKeyData = 650;
        itSelectKeyData.minKey = &minKeySelectKeyData;
        itSelectKeyData.maxKey = NULL;
        itSelectKeyData.minData = &minDataSelectKeyData;
        itSelectKeyData.maxData = &maxDataSelectKeyData;
        sbitsInitIterator(state, &itSelectKeyData);

        numRecordsSelectKeyData = 0;
        numReadsSelectKeyData = state->numReads;
        numIdxReadsSelectKeyData = state->numIdxReads;

        while (sbitsNext(state, &itSelectKeyData, recordBuffer, recordBuffer + state->keySize)) {
            numRecordsSelectKeyData++;
        }

        timeSelectKeyData[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000);

        numReadsSelectKeyData = state->numReads - numReadsSelectKeyData;
        numIdxReadsSelectKeyData = state->numIdxReads - numIdxReadsSelectKeyData;

        //////////////////////////
        // Sequential Key-Value //
        //////////////////////////
        fseek(dataset, 0, SEEK_SET);
        start = clock();

        numRecordsSeqKV = 0;
        numReadsSeqKV = state->numReads;

        while (fread(dataPage, 512, 1, dataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                sbitsGet(state, dataPage + record * state->recordSize, recordBuffer);
                numRecordsSeqKV++;
            }
        }

        timeSeqKV[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000);
        numReadsSeqKV = state->numReads - numReadsSeqKV;

        fclose(dataset);

        //////////////////////
        // Random Key-Value //
        //////////////////////
        FILE *randomDataset = fopen("data/uwa500K_randomized.bin", "rb");

        start = clock();

        numRecordsRandKV = 0;
        numReadsRandKV = state->numReads;

        while (fread(dataPage, 512, 1, randomDataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                sbitsGet(state, dataPage + record * state->recordSize, recordBuffer);
                numRecordsRandKV++;
            }
        }

        timeRandKV[run] = (clock() - start) / (CLOCKS_PER_SEC / 1000);
        numReadsRandKV = state->numReads - numReadsRandKV;

        fclose(randomDataset);

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

    // Calculate averages
    int sum = 0;
    printf("\nINSERT\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeInsert[i]);
        sum += timeInsert[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records inserted: %d\n", numRecords);
    printf("Num data pages written: %d\n", numWrites);
    printf("Num index pages written: %d\n", numIdxWrites);

    sum = 0;
    printf("\nSELECT * FROM r\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectAll[i]);
        sum += timeSelectAll[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSelectAll);
    printf("Num reads: %d\n", numReadsSelectAll);

    sum = 0;
    printf("\nSELECT * FROM r WHERE key >= 974100996\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectKeySmallResult[i]);
        sum += timeSelectKeySmallResult[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSelectKeySmallResult);
    printf("Num reads: %d\n", numReadsSelectKeySmallResult);

    sum = 0;
    printf("\nSELECT * FROM r WHERE key >= 949756644\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectKeyLargeResult[i]);
        sum += timeSelectKeyLargeResult[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSelectKeyLargeResult);
    printf("Num reads: %d\n", numReadsSelectKeyLargeResult);

    sum = 0;
    printf("\nSELECT * FROM r WHERE data >= 700\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectDataSmallResult[i]);
        sum += timeSelectDataSmallResult[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSelectDataSmallResult);
    printf("Num reads: %d\n", numReadsSelectDataSmallResult);
    printf("Num idx reads: %d\n", numIdxReadsSelectDataSmallResult);

    sum = 0;
    printf("\nSELECT * FROM r WHERE data >= 420\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectDataLargeResult[i]);
        sum += timeSelectDataLargeResult[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSelectDataLargeResult);
    printf("Num reads: %d\n", numReadsSelectDataLargeResult);
    printf("Num idx reads: %d\n", numIdxReadsSelectDataLargeResult);

    sum = 0;
    printf("\nSELECT * FROM r WHERE key >= 958885776 AND data >= 450 AND data <= 650\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectKeyData[i]);
        sum += timeSelectKeyData[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSelectKeyData);
    printf("Num reads: %d\n", numReadsSelectKeyData);
    printf("Num idx reads: %d\n", numIdxReadsSelectKeyData);

    sum = 0;
    printf("\nSequential Key-Value\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSeqKV[i]);
        sum += timeSeqKV[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsSeqKV);
    printf("Num reads: %d\n", numReadsSeqKV);

    sum = 0;
    printf("\nRandom Key-Value\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeRandKV[i]);
        sum += timeRandKV[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Result size: %d\n", numRecordsRandKV);
    printf("Num reads: %d\n", numReadsRandKV);

    return 0;
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
