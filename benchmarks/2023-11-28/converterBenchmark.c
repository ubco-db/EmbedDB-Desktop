#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../../src/embedDB/embedDB.h"
#include "../../src/embedDB/utilityFunctions.h"
#include "customFunctions.h"
#include "query1.h"
#include "query2.h"
#include "query3.h"
#include "query4.h"

#define NUM_RUNS 5

void runBenchmark(int queryNum);

void runQuery1();
void runQuery2();
void runQuery3();
void runQuery4();

void updateCustomUWABitmap(void *data, void *bm);
int8_t inCustomUWABitmap(void *data, void *bm);
void buildCustomUWABitmapFromRange(void *min, void *max, void *bm);

int main() {
    for (int i = 1; i <= 4; i++) {
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
    fclose(dataset);
    embedDBFlush(state);

    return state;
}

embedDBState *getSeededEthState() {
    embedDBState *state = (embedDBState *)calloc(1, sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = 12;
    state->compareKey = int32Comparator;
    state->compareData = customCol2Int32Comparator;
    state->pageSize = 512;
    state->eraseSizeInPages = 4;
    state->numSplinePoints = 300;
    state->numDataPages = 20000;
    state->numIndexPages = 100;
    state->fileInterface = getFileInterface();
    char dataPath[] = "../../build/artifacts/dataFile.bin", indexPath[] = "../../build/artifacts/indexFile.bin";
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);
    state->bufferSizeInBlocks = 4;
    state->buffer = calloc(state->bufferSizeInBlocks, state->pageSize);
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    state->bitmapSize = 1;
    state->inBitmap = inCustomEthBitmap;
    state->updateBitmap = updateCustomEthBitmap;
    state->buildBitmapFromRange = buildCustomEthBitmapFromRange;
    if (embedDBInit(state, 1)) {
        printf("Error initializing database\n");
        return NULL;
    }

    // Insert data
    char const dataFileName[] = "../../data/ethylene_CO_only_100K.bin";
    FILE *dataset = fopen(dataFileName, "rb");

    char dataPage[512];
    while (fread(dataPage, 512, 1, dataset)) {
        uint16_t count = *(uint16_t *)(dataPage + 4);
        for (int record = 1; record <= count; record++) {
            embedDBPut(state, dataPage + record * state->recordSize, dataPage + record * state->recordSize + state->keySize);
        }
    }
    fclose(dataset);
    embedDBFlush(state);

    return state;
}

embedDBState *getSeededWatchState() {
    embedDBState *state = calloc(1, sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = 12;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    state->pageSize = 512;
    state->eraseSizeInPages = 4;
    state->numDataPages = 20000;
    state->numIndexPages = 1000;
    state->numSplinePoints = 300;
    char dataPath[] = "dataFile.bin", indexPath[] = "indexFile.bin";
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);
    state->bufferSizeInBlocks = 4;
    state->buffer = malloc(state->bufferSizeInBlocks * state->pageSize);
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
    state->bitmapSize = 2;
    state->inBitmap = inCustomWatchBitmap;
    state->updateBitmap = updateCustomWatchBitmap;
    state->buildBitmapFromRange = buildCustomWatchBitmapFromRange;
    if (embedDBInit(state, 1)) {
        printf("Error initializing database\n");
        return NULL;
    }

    // Insert data
    char const dataFileName[] = "../../data/watch_only_100K.bin";
    FILE *dataset = fopen(dataFileName, "rb");

    char dataPage[512];
    while (fread(dataPage, 512, 1, dataset)) {
        uint16_t count = *(uint16_t *)(dataPage + 4);
        for (int record = 1; record <= count; record++) {
            embedDBPut(state, dataPage + record * state->recordSize, dataPage + record * state->recordSize + state->keySize);
        }
    }
    fclose(dataset);
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

void runQuery3() {
    double times[NUM_RUNS];
    int count = 0;

    for (int runNum = 0; runNum < NUM_RUNS; runNum++) {
        embedDBState *state = getSeededEthState();

        clock_t start = clock();

        count = execOperatorQuery3(state);

        clock_t end = clock();

        times[runNum] = (end - start) / (CLOCKS_PER_SEC / 1000.0);

        freeState(state);
    }

    double sum = 0;
    printf("Query 3: ");
    for (int i = 0; i < NUM_RUNS; i++) {
        sum += times[i];
        printf("%.1f, ", times[i]);
    }
    printf("\n");
    printf("Average: %.1fms\n", sum / NUM_RUNS);
    printf("Count: %d\n", count);
}

void runQuery4() {
    double times[NUM_RUNS];
    int count = 0;

    for (int runNum = 0; runNum < NUM_RUNS; runNum++) {
        embedDBState *state = getSeededWatchState();

        clock_t start = clock();

        count = execOperatorQuery4(state);

        clock_t end = clock();

        times[runNum] = (end - start) / (CLOCKS_PER_SEC / 1000.0);

        freeState(state);
    }

    double sum = 0;
    printf("Query 4: ");
    for (int i = 0; i < NUM_RUNS; i++) {
        sum += times[i];
        printf("%.1f, ", times[i]);
    }
    printf("\n");
    printf("Average: %.1fms\n", sum / NUM_RUNS);
    printf("Count: %d\n", count);
}
