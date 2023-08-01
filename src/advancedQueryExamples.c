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

void customShiftInit(sbitsOperator* operator) {
    operator->input->init(operator->input);
    operator->schema = copySchema(operator->input->schema);
    operator->recordBuffer = malloc(16);
}

int8_t customShiftNext(sbitsOperator* operator) {
    if (operator->input->next(operator->input)) {
        memcpy(operator->recordBuffer, operator->input->recordBuffer, 16);
        *(uint32_t*)operator->recordBuffer += 473385600;  // Add the number of seconds between 2000 and 2015
        return 1;
    }
    return 0;
}

void customShiftClose(sbitsOperator* operator) {
    operator->input->close(operator->input);
    sbitsFreeSchema(&operator->schema);
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

void insertData(sbitsState* state, char* filename);

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

    // Insert data
    insertData(state, "data/uwa500K.bin");

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
    sbitsFreeOperatorRecursive(&projOp1);

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
    sbitsAggrOp* counter = createCountAggregate();
    sbitsAggrOp* sum = createSumAggregate(8, -4);
    sbitsAggrOp aggrOperators[] = {groupName, *counter, *sum};
    uint32_t numOps = 3;
    sbitsOperator* aggOp3 = createAggregateOperator(selectOp3, sameDayGroup, aggrOperators, numOps);
    uint32_t minWindCount = 50;
    sbitsOperator* countSelect3 = createSelectionOperator(aggOp3, 1, SELECT_GT, &minWindCount);
    countSelect3->init(countSelect3);

    recordsReturned = 0;
    printLimit = 10000;
    recordBuffer = countSelect3->recordBuffer;
    printf("\nCount Result:\n");
    printf("Day        | Count | Sum\n");
    printf("-----------+-------+-----\n");
    while (exec(countSelect3)) {
        if (++recordsReturned < printLimit) {
            printf("%-10lu | %-5lu | %lld\n", recordBuffer[0], recordBuffer[1], ((int64_t*)recordBuffer)[1]);
        }
    }
    if (recordsReturned > printLimit) {
        printf("...\n");
        printf("[Total records returned: %d]\n", recordsReturned);
    }

    // Free states
    for (int i = 0; i < numOps; i++) {
        // free(aggrOperators[i].state);
    }

    countSelect3->close(countSelect3);
    sbitsFreeOperatorRecursive(&countSelect3);

    /** Create another table to demonstrate a proper join example
     *	Schema:
     *		- timestamp	uint32	PRIMARY KEY
     *		- airTemp	int32
     *		- airPres	int32
     *		- windSpeed	int32
     */
    sbitsState* state2 = malloc(sizeof(sbitsState));
    state2->keySize = 4;
    state2->dataSize = 12;
    state2->compareKey = int32Comparator;
    state2->compareData = int32Comparator;
    state2->pageSize = 512;
    state2->eraseSizeInPages = 4;
    state2->numDataPages = 20000;
    state2->numIndexPages = 1000;
    char dataPath2[] = "build/artifacts/dataFile2.bin", indexPath2[] = "build/artifacts/indexFile2.bin";
    state2->fileInterface = getFileInterface();
    state2->dataFile = setupFile(dataPath2);
    state2->indexFile = setupFile(indexPath2);
    state2->bufferSizeInBlocks = 4;
    state2->buffer = malloc(state2->bufferSizeInBlocks * state2->pageSize);
    state2->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX | SBITS_RESET_DATA;
    state2->bitmapSize = 2;
    state2->inBitmap = inBitmapInt16;
    state2->updateBitmap = updateBitmapInt16;
    state2->buildBitmapFromRange = buildBitmapInt16FromRange;
    sbitsInit(state2, 1);

    // Insert records
    insertData(state2, "data/sea100K.bin");

    /** Join
     *	uwa dataset has data from every minute, for the year 2000. sea dataset has samples every hour from 2011 to 2020. Let's compare the temperatures measured in 2000 in the uwa dataset and 2015 in the sea dataset
     */
    sbitsInitIterator(state, &it);  // Iterator on uwa
    sbitsIterator it2;
    uint32_t year2015 = 1420099200;      // 2015-01-01
    uint32_t year2016 = 1451635200 - 1;  // 2016-01-01
    it2.minKey = &year2015;
    it2.maxKey = &year2016;
    it2.minData = NULL;
    it2.maxData = NULL;
    sbitsInitIterator(state2, &it2);  // Iterator on sea

    // Prepare uwa table
    sbitsOperator* scan4_1 = createTableScanOperator(state, &it, baseSchema);
    sbitsOperator* shift4_1 = malloc(sizeof(sbitsOperator));  // Custom operator to shift the year 2000 to 2015 to make the join work
    shift4_1->input = scan4_1;
    shift4_1->init = customShiftInit;
    shift4_1->next = customShiftNext;
    shift4_1->close = customShiftClose;

    // Prepare sea table
    sbitsOperator* scan4_2 = createTableScanOperator(state2, &it2, baseSchema);
    scan4_2->init(scan4_2);

    // Join tables
    sbitsOperator* join4 = createKeyJoinOperator(shift4_1, scan4_2);

    // Project the timestamp and the two temp columns
    uint8_t projCols2[] = {0, 1, 5};
    sbitsOperator* proj4 = createProjectionOperator(join4, 3, projCols2);

    proj4->init(proj4);

    recordsReturned = 0;
    printLimit = 10;
    recordBuffer = proj4->recordBuffer;
    printf("\nCount Result:\n");
    printf("timestamp  | tmp_s | tmp_u\n");
    printf("-----------+-------+-------\n");
    while (exec(proj4)) {
        if (++recordsReturned < printLimit) {
            printf("%-10lu | %-5.1f | %-5.1f\n", recordBuffer[0], recordBuffer[1] / 10.0, recordBuffer[2] / 10.0);
        }
    }
    if (recordsReturned > printLimit) {
        printf("...\n");
        printf("[Total records returned: %d]\n", recordsReturned);
    }

    proj4->close(proj4);
    free(scan4_1);
    free(shift4_1);
    free(scan4_2);
    free(join4);
    free(proj4);

    // Close sbits
    sbitsClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
    free(state->fileInterface);
    free(state->buffer);
    free(state);
    sbitsClose(state2);
    tearDownFile(state2->dataFile);
    tearDownFile(state2->indexFile);
    free(state2->fileInterface);
    free(state2->buffer);
    free(state2);
}

void insertData(sbitsState* state, char* filename) {
    FILE* fp = fopen(filename, "rb");
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
}
