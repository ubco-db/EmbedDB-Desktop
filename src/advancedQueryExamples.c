#include <stdio.h>
#include <stdlib.h>

#include "sbits/advancedQueries.h"
#include "sbits/sbits.h"
#include "sbits/utilityFunctions.h"

int8_t projectionFunc(void* key, void* data) {
    memcpy((int8_t*)data + 4, (int8_t*)data + 8, 4);
    // Optional:
    // memset((int8_t*)data + 8, 0, 4);
    return 1;
}

int8_t selectionFunc(void* key, void* data) {
    return *(int32_t*)((int8_t*)data + 8) >= 200;
}

int8_t combinedFunc(void* key, void* data) {
    if (!selectionFunc(key, data)) {
        return 0;
    }
    return projectionFunc(key, data);
}

int8_t dayGroup(void* lastkey, void* key) {
    // Convert timestamp to correct timezone, then find the epoch day and compare them
    return (*(uint32_t*)lastkey) / 86400 == (*(uint32_t*)key) / 86400;
}

void countReset(void* state) {
    *(uint32_t*)state = 0;
}

void countAdd(void* state, void* key, void* data) {
    if (*(int32_t*)((int8_t*)data + 8) >= 150) {
        (*(uint32_t*)state)++;
    }
}

void countCompute(void* state) {}

void main() {
    sbitsState* state = calloc(1, sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 12;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    state->pageSize = 512;
    state->eraseSizeInPages = 4;
    state->numDataPages = 20000;
    state->numIndexPages = 1000;
    char dataPath[] = "build/artifacts/dataFile.bin", indexPath[] = "build/artifacts/indexFile.bin";
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);
    state->bufferSizeInBlocks = 4;
    state->buffer = malloc(state->bufferSizeInBlocks * state->pageSize);
    state->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX | SBITS_RESET_DATA;
    state->bitmapSize = 2;
    state->inBitmap = inBitmapInt16;
    state->updateBitmap = updateBitmapInt16;
    state->buildBitmapFromRange = buildBitmapInt16FromRange;
    sbitsInit(state, 1);

    // Insert data
    FILE* fp = fopen("data/uwa500K.bin", "rb");
    char fileBuffer[512];
    int numRecords = 0;
    while (fread(fileBuffer, state->pageSize, 1, fp)) {
        uint16_t count = SBITS_GET_COUNT(fileBuffer);
        for (int i = 1; i <= count; i++) {
            sbitsPut(state, fileBuffer + i * state->recordSize, fileBuffer + i * state->recordSize + state->keySize);
            numRecords++;
        }
    }
    fclose(fp);
    sbitsFlush(state);

    printf("\nInserted %d Records\n", numRecords);

    /**	Projection
     *	Dataset has three, 4 byte, data fields:
     *		- int32_t air temp X 10
     *		- int32_t air pressure X 10
     *		- int32_t wind speed
     *	Say we only want the air temp and wind speed columns to save memory in further processing. Right now the air pressure field is inbetween the two values we want, so we can simplify the process of extracting the desired colums using exec()
     */
    sbitsIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    sbitsInitIterator(state, &it);

    uint32_t keyBuffer = 0;
    int32_t dataBuffer[] = {0, 0, 0};

    int printLimit = 20;
    int recordsReturned = 0;
    printf("\nProjection Result:\n");
    printf("Time       | Temp | Wind Speed\n");
    printf("-----------+------+------------\n");
    while (exec(state, &it, projectionFunc, &keyBuffer, dataBuffer)) {
        if (++recordsReturned <= printLimit) {
            printf("%-10lu | %-4.1f | %-4.1f\n", keyBuffer, dataBuffer[0] / 10.0, dataBuffer[1] / 10.0);
        }
    }
    if (recordsReturned > printLimit) {
        printf("...\n");
        printf("[Total records returned: %d]\n", recordsReturned);
    }

    /**	Selection:
     *	Say we want to answer the question "Return records where the temperature is less than 40 degrees and the wind speed is greater than 20". The iterator is only indexing the data on temperature, so we need to apply an extra layer of selection to its return. We can take this even farther and combine the two functions into one: combinedFunc()
     */
    int32_t maxTemp = 400;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = &maxTemp;
    sbitsInitIterator(state, &it);

    recordsReturned = 0;
    printf("\nSelection Result:\n");
    printf("Time       | Temp | Wind Speed\n");
    printf("-----------+------+------------\n");
    while (exec(state, &it, combinedFunc, &keyBuffer, dataBuffer)) {
        if (++recordsReturned <= printLimit) {
            printf("%-10lu | %-4.1f | %-4.1f\n", keyBuffer, dataBuffer[0] / 10.0, dataBuffer[1] / 10.0);
        }
    }
    if (recordsReturned > printLimit) {
        printf("...\n");
        printf("[Total records returned: %d]\n", recordsReturned);
    }

    /**	Aggregate Count:
     * 	Count the number of records with wind speeds over 15 each day
     */
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    sbitsInitIterator(state, &it);

    sbitsAggrOp op1 = {countReset, countAdd, countCompute, malloc(sizeof(uint32_t))};
    sbitsAggrOp operators[] = {op1};

    recordsReturned = 0;
    printLimit = 365;
    printf("\nCount:\n");
    while (aggroup(state, &it, dayGroup, operators, 1, &keyBuffer, dataBuffer)) {
        if (++recordsReturned < printLimit) {
            printf("%d %d\n", keyBuffer / 86400 - 1, *(uint32_t*)op1.state);
        }
    }
    if (recordsReturned > printLimit) {
        printf("...\n");
        printf("[Total records returned: %d]\n", recordsReturned);
    }

    for (int i = 0; i < sizeof(operators) / sizeof(operators[0]); i++) {
        free(operators[i].state);
    }

    // Close sbits
    sbitsClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
    free(state->fileInterface);
    free(state->buffer);
    free(state);
}