#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../../src/embedDB/embedDB.h"
#include "../../src/embedDB/utilityFunctions.h"
#include "query1.h"
#include "query2.h"
#include "query3.h"
#include "query4.h"

#define NUM_RUNS 1

void runBenchmark(int queryNum);

void runQuery1();
void runQuery2();
void runQuery3();
void runQuery4();

void updateCustomUWABitmap(void *data, void *bm);
int8_t inCustomUWABitmap(void *data, void *bm);
void buildCustomUWABitmapFromRange(void *min, void *max, void *bm);

int main() {
    for (int i = 1; i <= 2; i++) {
        printf("\n");
        runBenchmark(i);
    }

    return 0;
}

void runBenchmark(int queryNum) {
    switch (queryNum) {
        case 1:
            runQuery1();
            break;
        case 2:
            runQuery2();
            break;
        case 3:
            runQuery3();
            break;
        case 4:
            runQuery4();
            break;
    }
}

embedDBState *getSeededUWAState() {
    embedDBState *state = (embedDBState *)calloc(1, sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = 12;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    state->pageSize = 512;
    state->eraseSizeInPages = 4;
    state->numSplinePoints = 30;
    state->numDataPages = 20000;
    state->numIndexPages = 100;
    state->fileInterface = getFileInterface();
    char dataPath[] = "../../build/artifacts/dataFile.bin", indexPath[] = "../../build/artifacts/indexFile.bin";
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);
    state->bufferSizeInBlocks = 4;
    state->buffer = calloc(state->bufferSizeInBlocks, state->pageSize);
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    state->bitmapSize = 2;
    state->inBitmap = inCustomUWABitmap;
    state->updateBitmap = updateCustomUWABitmap;
    state->buildBitmapFromRange = buildCustomUWABitmapFromRange;
    if (embedDBInit(state, 1)) {
        printf("Error initializing database\n");
        return NULL;
    }

    // Insert data
    char const dataFileName[] = "../../data/uwa500K_only_100K.bin";
    FILE *dataset = fopen(dataFileName, "rb");

    char dataPage[512];
    while (fread(dataPage, 512, 1, dataset)) {
        uint16_t count = *(uint16_t *)(dataPage + 4);
        for (int record = 1; record <= count; record++) {
            embedDBPut(state, dataPage + record * state->recordSize, dataPage + record * state->recordSize + state->keySize);
        }
    }
    embedDBFlush(state);

    return state;
}

void freeState(embedDBState *state) {
    embedDBClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
    free(state->fileInterface);
    free(state->buffer);
}

void runQuery1() {
    double times[NUM_RUNS];
    int count = 0;

    for (int runNum = 0; runNum < NUM_RUNS; runNum++) {
        embedDBState *state = getSeededUWAState();

        clock_t start = clock();

        count = execOperatorQuery1(state);

        clock_t end = clock();

        times[runNum] = (end - start) / (CLOCKS_PER_SEC / 1000.0);

        freeState(state);
    }

    double sum = 0;
    printf("Query 1: ");
    for (int i = 0; i < NUM_RUNS; i++) {
        sum += times[i];
        printf("%.1f, ", times[i]);
    }
    printf("\n");
    printf("Average: %.1fms\n", sum / NUM_RUNS);
    printf("Count: %d\n", count);
}

void runQuery2() {
    double times[NUM_RUNS];
    int count = 0;

    for (int runNum = 0; runNum < NUM_RUNS; runNum++) {
        embedDBState *state = getSeededUWAState();

        clock_t start = clock();

        count = execOperatorQuery2(state);

        clock_t end = clock();

        times[runNum] = (end - start) / (CLOCKS_PER_SEC / 1000.0);

        freeState(state);
    }

    double sum = 0;
    printf("Query 2: ");
    for (int i = 0; i < NUM_RUNS; i++) {
        sum += times[i];
        printf("%.1f, ", times[i]);
    }
    printf("\n");
    printf("Average: %.1fms\n", sum / NUM_RUNS);
    printf("Count: %d\n", count);
}

void runQuery3() {}
void runQuery4() {}

void updateCustomUWABitmap(void *data, void *bm) {
    int32_t temp = *(int32_t *)data;

    /*  Custom, equi-depth buckets
     *  Bucket  0: 315 -> 373 (0, 6249)
     *	Bucket  1: 373 -> 385 (6250, 12499)
     *	Bucket  2: 385 -> 398 (12500, 18749)
     *	Bucket  3: 398 -> 408 (18750, 24999)
     *	Bucket  4: 408 -> 416 (25000, 31249)
     *	Bucket  5: 416 -> 423 (31250, 37499)
     *	Bucket  6: 423 -> 429 (37500, 43749)
     *	Bucket  7: 429 -> 435 (43750, 49999)
     *	Bucket  8: 435 -> 443 (50000, 56249)
     *	Bucket  9: 443 -> 449 (56250, 62499)
     *	Bucket 10: 449 -> 456 (62500, 68749)
     *	Bucket 11: 456 -> 464 (68750, 74999)
     *	Bucket 12: 464 -> 473 (75000, 81249)
     *	Bucket 13: 473 -> 484 (81250, 87499)
     *	Bucket 14: 484 -> 500 (87500, 93749)
     *	Bucket 15: 500 -> 602 (93750, 99999)
     */

    uint16_t mask = 1;

    int8_t mode = 0;  // 0 = equi-depth, 1 = equi-width
    if (mode == 0) {
        if (temp < 373) {
            mask <<= 0;
        } else if (temp < 385) {
            mask <<= 1;
        } else if (temp < 398) {
            mask <<= 2;
        } else if (temp < 408) {
            mask <<= 3;
        } else if (temp < 416) {
            mask <<= 4;
        } else if (temp < 423) {
            mask <<= 5;
        } else if (temp < 429) {
            mask <<= 6;
        } else if (temp < 435) {
            mask <<= 7;
        } else if (temp < 443) {
            mask <<= 8;
        } else if (temp < 449) {
            mask <<= 9;
        } else if (temp < 456) {
            mask <<= 10;
        } else if (temp < 464) {
            mask <<= 11;
        } else if (temp < 473) {
            mask <<= 12;
        } else if (temp < 484) {
            mask <<= 13;
        } else if (temp < 500) {
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
