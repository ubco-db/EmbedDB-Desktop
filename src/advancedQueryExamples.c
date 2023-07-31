#include <stdio.h>
#include <stdlib.h>

#include "sbits/advancedQueries.h"
#include "sbits/sbits.h"
#include "sbits/utilityFunctions.h"

int8_t mySelectionFunc(sbitsState* state, void* info, void* key, void* data) {
    return (*(uint32_t*)key) / 86400 == *(uint32_t*)info && *((int32_t*)data + 2) >= 150;
}

uint32_t dayGroup(const void* record) {
    // find the epoch day
    return (*(uint32_t*)record) / 86400;
}

int8_t sameDayGroup(const void* lastRecord, const void* record) {
    return dayGroup(lastRecord) == dayGroup(record);
}

void writeDayGroup(sbitsAggrOp* aggrOp, sbitsSchema* schema, void* recordBuffer, const void* lastRecord) {
    // Put day in record
    uint32_t day = dayGroup(lastRecord);
    memcpy((int8_t*)recordBuffer + getColPosFromSchema(schema, aggrOp->colNum), &day, sizeof(uint32_t));
}

void countReset(sbitsAggrOp* aggrOp) {
    *(uint32_t*)aggrOp->state = 0;
}

void countAdd(sbitsAggrOp* aggrOp, const void* recordBuffer) {
    (*(uint32_t*)aggrOp->state)++;
}

void countCompute(sbitsAggrOp* aggrOp, sbitsSchema* schema, void* recordBuffer, const void* lastRecord) {
    // Put count in record
    memcpy((int8_t*)recordBuffer + getColPosFromSchema(schema, aggrOp->colNum), aggrOp->state, sizeof(uint32_t));
}

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

    sbitsIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    sbitsInitIterator(state, &it);

    sbitsOperator* scanOp1 = createTableScanOperator(state, &it, baseSchema);
    uint8_t projCols[] = {0, 1, 3};
    sbitsOperator* projOp1 = createProjectionOperator(scanOp1, 3, projCols);
    projOp1->init(projOp1);

    int printLimit = 20;
    int recordsReturned = 0;
    int32_t* recordBuffer = projOp1->recordBuffer;
    printf("\nProjection Result:\n");
    printf("Time       | Temp | Wind Speed\n");
    printf("-----------+------+------------\n");
    while (exec(projOp1)) {
        if (++recordsReturned <= printLimit) {
            printf("%-10lu | %-4.1f | %-4.1f\n", recordBuffer[0], recordBuffer[1] / 10.0, recordBuffer[2] / 10.0);
        }
    }
    if (recordsReturned > printLimit) {
        printf("...\n");
        printf("[Total records returned: %d]\n", recordsReturned);
    }

    projOp1->close(projOp1);
    sbitsFreeOperatorRecursive(projOp1);

    /**	Selection:
     *	Say we want to answer the question "Return records where the temperature is less than or equal to 40 degrees and the wind speed is greater than or equal to 20". The iterator is only indexing the data on temperature, so we need to apply an extra layer of selection to its return. We can then apply the projection on top of that output
     */
    int32_t maxTemp = 400;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = &maxTemp;
    sbitsInitIterator(state, &it);

    sbitsOperator* scanOp2 = createTableScanOperator(state, &it, baseSchema);
    int32_t selVal = 200;
    sbitsOperator* selectOp2 = createSelectionOperator(scanOp2, 3, SELECT_GTE, &selVal);
    sbitsOperator* projOp2 = createProjectionOperator(selectOp2, 3, projCols);
    projOp2->init(projOp2);

    recordsReturned = 0;
    recordBuffer = projOp2->recordBuffer;
    printf("\nSelection Result:\n");
    printf("Time       | Temp | Wind Speed\n");
    printf("-----------+------+------------\n");
    while (exec(projOp2)) {
        if (++recordsReturned <= printLimit) {
            printf("%-10lu | %-4.1f | %-4.1f\n", recordBuffer[0], recordBuffer[1] / 10.0, recordBuffer[2] / 10.0);
        }
    }
    if (recordsReturned > printLimit) {
        printf("...\n");
        printf("[Total records returned: %d]\n", recordsReturned);
    }

    projOp2->close(projOp2);
    sbitsFreeOperatorRecursive(&projOp2);

    /**	Aggregate Count:
     * 	Get days in which there were at least 50 minutes of wind measurements over 15
     */
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    sbitsInitIterator(state, &it);

    sbitsOperator* scanOp3 = createTableScanOperator(state, &it, baseSchema);
    selVal = 150;
    sbitsOperator* selectOp3 = createSelectionOperator(scanOp3, 3, SELECT_GTE, &selVal);
    sbitsAggrOp groupName = {NULL, NULL, writeDayGroup, NULL, 4};
    void* buf = malloc(sizeof(uint32_t));
    sbitsAggrOp counter = {countReset, countAdd, countCompute, buf, 4};
    sbitsAggrOp aggrOperators[] = {groupName, counter};
    uint32_t numOps = 2;
    sbitsOperator* aggOp3 = createAggregateOperator(selectOp3, sameDayGroup, aggrOperators, numOps);
    uint32_t minWindCount = 50;
    sbitsOperator* countSelect3 = createSelectionOperator(aggOp3, 1, SELECT_GT, &minWindCount);
    countSelect3->init(countSelect3);

    recordsReturned = 0;
    printLimit = 10000;
    recordBuffer = countSelect3->recordBuffer;
    printf("\nCount Result:\n");
    printf("Day        | Count \n");
    printf("-----------+-------\n");
    while (exec(countSelect3)) {
        if (++recordsReturned < printLimit) {
            printf("%-10lu | %-4lu\n", recordBuffer[0], recordBuffer[1]);
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

    countSelect3->close(countSelect3);
    sbitsFreeOperatorRecursive(countSelect3);

    // Close sbits
    sbitsClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
    free(state->fileInterface);
    free(state->buffer);
    free(state);
}
