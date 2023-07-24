#include <stdio.h>
#include <stdlib.h>

#include "sbits/advancedQueries.h"
#include "sbits/sbits.h"
#include "sbits/utilityFunctions.h"

int8_t mySelectionFunc(sbitsState* state, void* info, void* key, void* data) {
    return (*(uint32_t*)key) / 86400 == *(uint32_t*)info && *((int32_t*)data + 2) >= 150;
}

int8_t dayGroup(void* lastkey, void* key) {
    // Convert timestamp to correct timezone, then find the epoch day and compare them
    return (*(uint32_t*)lastkey) / 86400 == (*(uint32_t*)key) / 86400;
}

void countReset(void* state) {
    *(uint32_t*)state = 0;
}

void countAdd(void* state, void* recordBuffer) {
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

    int8_t colSizes[] = {4, 4, 4, 4};
    int8_t colSignedness[] = {SBITS_COLUMN_UNSIGNED, SBITS_COLUMN_SIGNED, SBITS_COLUMN_SIGNED, SBITS_COLUMN_SIGNED};
    sbitsSchema* baseSchema = sbitsCreateSchema(4, colSizes, colSignedness);
    sbitsSchema* schemaBuffer = malloc(sizeof(sbitsSchema));  // Allocate space for schema
    schemaBuffer->columnSizes = malloc(1);                    // This just needs to be an allocated space. It will be realloced by operators that modify the schema

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
    int32_t recordBuffer[4];

    sbitsIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    sbitsInitIterator(state, &it);

    sbitsOperator scanOp1 = {NULL, tableScan, createTableScanInfo(state, &it, baseSchema)};
    uint8_t projCols[] = {0, 1, 3};
    sbitsOperator projOp1 = {&scanOp1, projectionFunc, createProjectionInfo(3, projCols)};

    int printLimit = 20;
    int recordsReturned = 0;
    printf("\nProjection Result:\n");
    printf("Time       | Temp | Wind Speed\n");
    printf("-----------+------+------------\n");
    while (exec(&projOp1, schemaBuffer, recordBuffer)) {
        if (++recordsReturned <= printLimit) {
            printf("%-10lu | %-4.1f | %-4.1f\n", recordBuffer[0], recordBuffer[1] / 10.0, recordBuffer[2] / 10.0);
        }
    }
    if (recordsReturned > printLimit) {
        printf("...\n");
        printf("[Total records returned: %d]\n", recordsReturned);
    }

    free(scanOp1.info);
    free(projOp1.info);

    /**	Selection:
     *	Say we want to answer the question "Return records where the temperature is less than 40 degrees and the wind speed is greater than or equal to 20". The iterator is only indexing the data on temperature, so we need to apply an extra layer of selection to its return. We can then apply the projection on top of that output
     */
    int32_t maxTemp = 400;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = &maxTemp;
    sbitsInitIterator(state, &it);

    sbitsOperator scanOp2 = {NULL, tableScan, createTableScanInfo(state, &it, baseSchema)};
    int32_t selVal = 200;
    sbitsOperator selectOp2 = {&scanOp2, selectionFunc, createSelectinfo(3, SELECT_GTE, &selVal)};
    sbitsOperator projOp2 = {&selectOp2, projectionFunc, createProjectionInfo(3, projCols)};

    recordsReturned = 0;
    printf("\nSelection Result:\n");
    printf("Time       | Temp | Wind Speed\n");
    printf("-----------+------+------------\n");
    while (exec(&projOp2, schemaBuffer, recordBuffer)) {
        if (++recordsReturned <= printLimit) {
            printf("%-10lu | %-4.1f | %-4.1f\n", recordBuffer[0], recordBuffer[1] / 10.0, recordBuffer[2] / 10.0);
        }
    }
    if (recordsReturned > printLimit) {
        printf("...\n");
        printf("[Total records returned: %d]\n", recordsReturned);
    }

    free(scanOp2.info);
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

    memset(recordBuffer, 0xff, 4 * sizeof(int32_t));  // Set flag for first query
    sbitsOperator scanOp3 = {NULL, tableScan, createTableScanInfo(state, &it, baseSchema)};
    selVal = 150;
    sbitsOperator selectOp3 = {&scanOp3, selectionFunc, createSelectinfo(3, SELECT_GTE, &selVal)};
    sbitsAggrOp op1 = {countReset, countAdd, countCompute, malloc(sizeof(uint32_t))};
    sbitsAggrOp aggrOperators[] = {op1};
    uint32_t numOps = 1;

    recordsReturned = 0;
    printf("\nCount:\n");
    while (aggroup(&selectOp3, dayGroup, aggrOperators, numOps, schemaBuffer, recordBuffer, 16)) {
        if (++recordsReturned < printLimit) {
            printf("%d\n", *(uint32_t*)op1.state);
        }
    }
    if (recordsReturned > printLimit) {
        printf("...\n");
        printf("[Total records returned: %d]\n", recordsReturned);
    }

    // Free states
    for (int i = 0; i < numOps; i++) {
        free(aggrOperators[i].state);
    }
    free(scanOp3.info);
    free(selectOp3.info);

    // Close sbits
    sbitsClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
    free(state->fileInterface);
    free(state->buffer);
    free(state);
}
