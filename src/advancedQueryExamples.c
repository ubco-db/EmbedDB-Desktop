#include <stdio.h>
#include <stdlib.h>

#include "sbits/advancedQueries.h"
#include "sbits/sbits.h"
#include "sbits/utilityFunctions.h"

int8_t projectionFunc(sbitsState* state, void* info, void* key, void* data) {
    uint8_t numCols = *(uint8_t*)info;
    uint8_t* cols = (uint8_t*)info + 1;
    for (uint8_t i = 0; i < numCols; i++) {
        memcpy((int8_t*)data + i * 4, (int8_t*)data + cols[i] * 4, 4);
    }
    memset((int8_t*)data + numCols * 4, 0, state->dataSize - numCols * 4);
    return 1;
}
void* createProjectionInfo(uint8_t numCols, uint8_t* cols) {
    uint8_t* info = malloc(numCols + 1);
    info[0] = numCols;
    memcpy(info + 1, cols, numCols);
    return info;
}

int8_t mySelectionFunc(sbitsState* state, void* info, void* key, void* data) {
    return *(int32_t*)((int8_t*)data + 8) >= 200;
}
#define SELECT_GT 0
#define SELECT_LT 1
#define SELECT_GTE 2
#define SELECT_LTE 3
int8_t selectionFunc(sbitsState* state, void* info, void* key, void* data) {
    int8_t colNum = *(int8_t*)info;
    int8_t operation = *((int8_t*)info + 1);
    int32_t compVal = *(int32_t*)((int8_t*)info + 2);
    switch (operation) {
        case SELECT_GT:
            return *(int32_t*)((int8_t*)data + colNum * 4) > compVal;
        case SELECT_LT:
            return *(int32_t*)((int8_t*)data + colNum * 4) < compVal;
        case SELECT_GTE:
            return *(int32_t*)((int8_t*)data + colNum * 4) >= compVal;
        case SELECT_LTE:
            return *(int32_t*)((int8_t*)data + colNum * 4) <= compVal;
        default:
            return 0;
    }
}
void* createSelectinfo(int8_t colNum, int8_t operation, int32_t compVal) {
    int8_t* data = malloc(6);
    data[0] = colNum;
    data[1] = operation;
    memcpy(data + 2, &compVal, 4);
    return data;
}

int8_t dayGroup(void* lastkey, void* key) {
    // Convert timestamp to correct timezone, then find the epoch day and compare them
    return (*(uint32_t*)lastkey) / 86400 == (*(uint32_t*)key) / 86400;
}

void countReset(void* state) {
    *(uint32_t*)state = 0;
}

void countAdd(void* state, void* key, void* data) {
    (*(uint32_t*)state)++;
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
    uint32_t keyBuffer;
    int32_t dataBuffer[3];

    sbitsIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    sbitsInitIterator(state, &it);

    sbitsOperator scanOp1 = {NULL, sbitsNext, &it};
    uint8_t projCols[] = {0, 2};
    sbitsOperator projOp1 = {&scanOp1, projectionFunc, createProjectionInfo(2, projCols)};

    int printLimit = 20;
    int recordsReturned = 0;
    printf("\nProjection Result:\n");
    printf("Time       | Temp | Wind Speed\n");
    printf("-----------+------+------------\n");
    while (exec(state, &projOp1, &keyBuffer, dataBuffer)) {
        if (++recordsReturned <= printLimit) {
            printf("%-10lu | %-4.1f | %-4.1f\n", keyBuffer, dataBuffer[0] / 10.0, dataBuffer[1] / 10.0);
        }
    }
    if (recordsReturned > printLimit) {
        printf("...\n");
        printf("[Total records returned: %d]\n", recordsReturned);
    }

    free(projOp1.info);

    /**	Selection:
     *	Say we want to answer the question "Return records where the temperature is less than 40 degrees and the wind speed is greater than 20". The iterator is only indexing the data on temperature, so we need to apply an extra layer of selection to its return. We can take this even farther and combine the two functions into one: combinedFunc()
     */
    int32_t maxTemp = 400;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = &maxTemp;
    sbitsInitIterator(state, &it);

    sbitsOperator scanOp2 = {NULL, sbitsNext, &it};
    sbitsOperator selectOp2 = {&scanOp2, mySelectionFunc, NULL};
    sbitsOperator projOp2 = {&selectOp2, projectionFunc, createProjectionInfo(2, projCols)};

    recordsReturned = 0;
    printf("\nSelection Result:\n");
    printf("Time       | Temp | Wind Speed\n");
    printf("-----------+------+------------\n");
    while (exec(state, &projOp2, &keyBuffer, dataBuffer)) {
        if (++recordsReturned <= printLimit) {
            printf("%-10lu | %-4.1f | %-4.1f\n", keyBuffer, dataBuffer[0] / 10.0, dataBuffer[1] / 10.0);
        }
    }
    if (recordsReturned > printLimit) {
        printf("...\n");
        printf("[Total records returned: %d]\n", recordsReturned);
    }

    free(selectOp2.info);
    free(projOp2.info);

    /**	Aggregate Count:
     * 	Count the number of records with wind speeds over 15 each day
     */
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    sbitsInitIterator(state, &it);

    keyBuffer = UINT32_MAX;
    sbitsOperator scanOp3 = {NULL, sbitsNext, &it};
    sbitsOperator selectOp3 = {&scanOp3, selectionFunc, createSelectinfo(2, SELECT_GTE, 150)};
    sbitsAggrOp op1 = {countReset, countAdd, countCompute, malloc(sizeof(uint32_t))};
    sbitsAggrOp operators[] = {op1};

    recordsReturned = 0;
    printf("\nCount:\n");
    while (aggroup(state, &selectOp3, dayGroup, operators, 1, &keyBuffer, dataBuffer)) {
        if (++recordsReturned < printLimit) {
            printf("%d %d\n", keyBuffer / 86400 - 1, *(uint32_t*)op1.state);
        }
    }
    if (recordsReturned > printLimit) {
        printf("...\n");
        printf("[Total records returned: %d]\n", recordsReturned);
    }

    // Free states
    for (int i = 0; i < sizeof(operators) / sizeof(operators[0]); i++) {
        free(operators[i].state);
    }
    free(selectOp3.info);

    // Close sbits
    sbitsClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
    free(state->fileInterface);
    free(state->buffer);
    free(state);
}