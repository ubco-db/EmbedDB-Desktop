#include <math.h>
#include <string.h>

#include "../src/sbits/advancedQueries.h"
#include "../src/sbits/sbits.h"
#include "../src/sbits/utilityFunctions.h"
#include "unity.h"

typedef struct DataSource {
    FILE* fp;
    int8_t* pageBuffer;
    int32_t pageRecord;
} DataSource;

void insertData(sbitsState* state, char* filename);
void* nextRecord(DataSource* source);
uint32_t dayGroup(const void* record);
int8_t sameDayGroup(const void* lastRecord, const void* record);
void writeDayGroup(sbitsAggregateFunc* aggFunc, sbitsSchema* schema, void* recordBuffer, const void* lastRecord);
void customShiftInit(sbitsOperator* operator);
int8_t customShiftNext(sbitsOperator* operator);
void customShiftClose(sbitsOperator* operator);

sbitsState *stateUWA, *stateSEA;
sbitsSchema* baseSchema;
DataSource *uwaData, *seaData;

void setUp(void) {
    // Init UWA dataset
    stateUWA = calloc(1, sizeof(sbitsState));
    stateUWA->keySize = 4;
    stateUWA->dataSize = 12;
    stateUWA->compareKey = int32Comparator;
    stateUWA->compareData = int32Comparator;
    stateUWA->pageSize = 512;
    stateUWA->eraseSizeInPages = 4;
    stateUWA->numDataPages = 20000;
    stateUWA->numIndexPages = 1000;
    char dataPath[] = "build/artifacts/dataFile.bin", indexPath[] = "build/artifacts/indexFile.bin";
    stateUWA->fileInterface = getFileInterface();
    stateUWA->dataFile = setupFile(dataPath);
    stateUWA->indexFile = setupFile(indexPath);
    stateUWA->bufferSizeInBlocks = 4;
    stateUWA->buffer = malloc(stateUWA->bufferSizeInBlocks * stateUWA->pageSize);
    stateUWA->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX | SBITS_RESET_DATA;
    stateUWA->bitmapSize = 2;
    stateUWA->inBitmap = inBitmapInt16;
    stateUWA->updateBitmap = updateBitmapInt16;
    stateUWA->buildBitmapFromRange = buildBitmapInt16FromRange;
    sbitsInit(stateUWA, 1);
    insertData(stateUWA, "data/uwa500K.bin");

    // Init SEA dataset
    stateSEA = calloc(1, sizeof(sbitsState));
    stateSEA->keySize = 4;
    stateSEA->dataSize = 12;
    stateSEA->compareKey = int32Comparator;
    stateSEA->compareData = int32Comparator;
    stateSEA->pageSize = 512;
    stateSEA->eraseSizeInPages = 4;
    stateSEA->numDataPages = 20000;
    stateSEA->numIndexPages = 1000;
    char dataPath2[] = "build/artifacts/dataFile2.bin", indexPath2[] = "build/artifacts/indexFile2.bin";
    stateSEA->fileInterface = getFileInterface();
    stateSEA->dataFile = setupFile(dataPath2);
    stateSEA->indexFile = setupFile(indexPath2);
    stateSEA->bufferSizeInBlocks = 4;
    stateSEA->buffer = malloc(stateSEA->bufferSizeInBlocks * stateSEA->pageSize);
    stateSEA->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX | SBITS_RESET_DATA;
    stateSEA->bitmapSize = 2;
    stateSEA->inBitmap = inBitmapInt16;
    stateSEA->updateBitmap = updateBitmapInt16;
    stateSEA->buildBitmapFromRange = buildBitmapInt16FromRange;
    sbitsInit(stateSEA, 1);
    insertData(stateSEA, "data/sea100K.bin");

    // Init base schema
    int8_t colSizes[] = {4, 4, 4, 4};
    int8_t colSignedness[] = {SBITS_COLUMN_UNSIGNED, SBITS_COLUMN_SIGNED, SBITS_COLUMN_SIGNED, SBITS_COLUMN_SIGNED};
    baseSchema = sbitsCreateSchema(4, colSizes, colSignedness);

    // Open data sources for comparison
    uwaData = malloc(sizeof(DataSource));
    uwaData->fp = fopen("data/uwa500K.bin", "rb");
    uwaData->pageBuffer = calloc(1, 512);
    uwaData->pageRecord = 0;

    seaData = malloc(sizeof(DataSource));
    seaData->fp = fopen("data/sea100K.bin", "rb");
    seaData->pageBuffer = calloc(1, 512);
    seaData->pageRecord = 0;
}

void test_projection() {
    sbitsIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    sbitsInitIterator(stateUWA, &it);

    sbitsOperator* scanOp = createTableScanOperator(stateUWA, &it, baseSchema);
    uint8_t projCols[] = {0, 1, 3};
    sbitsOperator* projOp = createProjectionOperator(scanOp, 3, projCols);
    projOp->init(projOp);

    TEST_ASSERT_EQUAL_UINT8_MESSAGE(3, projOp->schema->numCols, "Output schema has wrong number of columns");
    int8_t expectedColSizes[] = {4, -4, -4};
    TEST_ASSERT_EQUAL_INT8_ARRAY_MESSAGE(expectedColSizes, projOp->schema->columnSizes, 3, "Output schema col sizes are wrong");

    int32_t recordsReturned = 0;
    int32_t* recordBuffer = projOp->recordBuffer;
    while (exec(projOp)) {
        recordsReturned++;
        int32_t* expectedRecord = (int32_t*)nextRecord(uwaData);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedRecord[0], recordBuffer[0], "First column is wrong");
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedRecord[1], recordBuffer[1], "Second column is wrong");
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedRecord[3], recordBuffer[2], "Third column is wrong");
    }

    projOp->close(projOp);
    sbitsFreeOperatorRecursive(&projOp);

    TEST_ASSERT_EQUAL_INT32_MESSAGE(500000, recordsReturned, "Projection didn't return the right number of records");
}

void test_selection() {
    int32_t maxTemp = 400;
    sbitsIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = &maxTemp;
    sbitsInitIterator(stateUWA, &it);

    sbitsOperator* scanOp = createTableScanOperator(stateUWA, &it, baseSchema);
    int32_t selVal = 200;
    sbitsOperator* selectOp = createSelectionOperator(scanOp, 3, SELECT_GTE, &selVal);
    uint8_t projCols[] = {0, 1, 3};
    sbitsOperator* projOp = createProjectionOperator(selectOp, 3, projCols);
    projOp->init(projOp);

    int32_t recordsReturned = 0;
    int32_t* recordBuffer = projOp->recordBuffer;
    while (exec(projOp)) {
        recordsReturned++;
        int32_t* expectedRecord = (int32_t*)nextRecord(uwaData);
        while (expectedRecord[1] > maxTemp || expectedRecord[3] < selVal) {
            expectedRecord = (int32_t*)nextRecord(uwaData);
        }
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedRecord[0], recordBuffer[0], "First column is wrong");
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedRecord[1], recordBuffer[1], "Second column is wrong");
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedRecord[3], recordBuffer[2], "Third column is wrong");
    }

    projOp->close(projOp);
    sbitsFreeOperatorRecursive(&projOp);

    TEST_ASSERT_EQUAL_INT32_MESSAGE(4, recordsReturned, "Selection didn't return the right number of records");
}

void test_aggregate() {
    sbitsIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    sbitsInitIterator(stateUWA, &it);

    sbitsOperator* scanOp = createTableScanOperator(stateUWA, &it, baseSchema);
    int32_t selVal = 150;
    sbitsOperator* selectOp = createSelectionOperator(scanOp, 3, SELECT_GTE, &selVal);
    sbitsAggregateFunc groupName = {NULL, NULL, writeDayGroup, NULL, 4};
    sbitsAggregateFunc* counter = createCountAggregate();
    sbitsAggregateFunc* sum = createSumAggregate(2);
    sbitsAggregateFunc aggFunctions[] = {groupName, *counter, *sum};
    uint32_t functionsLength = 3;
    sbitsOperator* aggOp = createAggregateOperator(selectOp, sameDayGroup, aggFunctions, functionsLength);
    aggOp->init(aggOp);

    int32_t recordsReturned = 0;
    int32_t* recordBuffer = aggOp->recordBuffer;
    while (exec(aggOp)) {
        recordsReturned++;
        int32_t* expectedRecord = (int32_t*)nextRecord(uwaData);
        while (expectedRecord[3] < selVal) {
            expectedRecord = (int32_t*)nextRecord(uwaData);
        }
        uint32_t firstInGroupDay = dayGroup(expectedRecord);
        uint32_t day = 0;
        uint32_t count = 0;
        int64_t sum = 0;
        do {
            count++;
            sum += expectedRecord[2];
            expectedRecord = (int32_t*)nextRecord(uwaData);
            while (expectedRecord[3] < selVal) {
                expectedRecord = (int32_t*)nextRecord(uwaData);
                if (expectedRecord == NULL) {
                    goto done;
                }
            }
            day = dayGroup(expectedRecord);
        } while (day == firstInGroupDay);
    done:
        uwaData->pageRecord--;
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(firstInGroupDay, recordBuffer[0], "Group label is wrong");
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(count, recordBuffer[1], "Count is wrong");
        TEST_ASSERT_EQUAL_INT64_MESSAGE(sum, ((int64_t*)recordBuffer)[1], "Sum is wrong");
    }

    // Free states
    for (int i = 0; i < functionsLength; i++) {
        if (aggFunctions[i].state != NULL) {
            free(aggFunctions[i].state);
        }
    }

    aggOp->close(aggOp);
    sbitsFreeOperatorRecursive(&aggOp);

    TEST_ASSERT_EQUAL_INT32_MESSAGE(90, recordsReturned, "Aggregate didn't return the right number of records");
}

void test_join() {
    sbitsIterator it;
    it.minKey = NULL;
    it.maxKey = NULL;
    it.minData = NULL;
    it.maxData = NULL;
    sbitsIterator it2;
    uint32_t year2015 = 1420099200;      // 2015-01-01
    uint32_t year2016 = 1451635200 - 1;  // 2016-01-01
    it2.minKey = &year2015;
    it2.maxKey = &year2016;
    it2.minData = NULL;
    it2.maxData = NULL;
    sbitsInitIterator(stateUWA, &it);   // Iterator on uwa
    sbitsInitIterator(stateSEA, &it2);  // Iterator on sea

    // Prepare uwa table
    sbitsOperator* scan1 = createTableScanOperator(stateUWA, &it, baseSchema);
    sbitsOperator* shift = malloc(sizeof(sbitsOperator));  // Custom operator to shift the year 2000 to 2015 to make the join work
    shift->input = scan1;
    shift->init = customShiftInit;
    shift->next = customShiftNext;
    shift->close = customShiftClose;

    // Prepare sea table
    sbitsOperator* scan2 = createTableScanOperator(stateSEA, &it2, baseSchema);
    scan2->init(scan2);

    // Join tables
    sbitsOperator* join = createKeyJoinOperator(shift, scan2);

    // Project the timestamp and the two temp columns
    uint8_t projCols2[] = {0, 1, 5};
    sbitsOperator* proj = createProjectionOperator(join, 3, projCols2);

    proj->init(proj);

    int32_t recordsReturned = 0;
    int32_t* recordBuffer = proj->recordBuffer;

    FILE* fp = fopen("test/TestAdvancedQueries_test_join_expected.bin", "rb");
    int32_t expectedRecord[3];
    while (exec(proj)) {
        recordsReturned++;
        fread(expectedRecord, sizeof(int32_t), 3, fp);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedRecord[0], recordBuffer[0], "First column is wrong");
        TEST_ASSERT_EQUAL_INT32_MESSAGE(expectedRecord[1], recordBuffer[1], "Second column is wrong");
        TEST_ASSERT_EQUAL_INT32_MESSAGE(expectedRecord[2], recordBuffer[2], "Third column is wrong");
    }
    fclose(fp);

    proj->close(proj);
    free(scan1);
    free(shift);
    free(scan2);
    free(join);
    free(proj);

    TEST_ASSERT_EQUAL_INT32_MESSAGE(9942, recordsReturned, "Join didn't return the right number of records");
}

void tearDown(void) {
    sbitsClose(stateUWA);
    tearDownFile(stateUWA->dataFile);
    tearDownFile(stateUWA->indexFile);
    free(stateUWA->fileInterface);
    free(stateUWA->buffer);
    free(stateUWA);

    sbitsClose(stateSEA);
    tearDownFile(stateSEA->dataFile);
    tearDownFile(stateSEA->indexFile);
    free(stateSEA->fileInterface);
    free(stateSEA->buffer);
    free(stateSEA);

    sbitsFreeSchema(&baseSchema);

    fclose(uwaData->fp);
    free(uwaData->pageBuffer);
    free(uwaData);
    fclose(seaData->fp);
    free(seaData->pageBuffer);
    free(seaData);
}

int main(void) {
    UNITY_BEGIN();

    RUN_TEST(test_projection);
    RUN_TEST(test_selection);
    RUN_TEST(test_aggregate);
    RUN_TEST(test_join);

    UNITY_END();
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
}

void* nextRecord(DataSource* source) {
    uint16_t count = SBITS_GET_COUNT(source->pageBuffer);
    if (count <= source->pageRecord) {
        // Read next page
        if (!fread(source->pageBuffer, 512, 1, source->fp)) {
            return NULL;
        }
        source->pageRecord = 0;
    }
    return source->pageBuffer + ++source->pageRecord * 16;
}

uint32_t dayGroup(const void* record) {
    // find the epoch day
    return (*(uint32_t*)record) / 86400;
}

int8_t sameDayGroup(const void* lastRecord, const void* record) {
    return dayGroup(lastRecord) == dayGroup(record);
}

void writeDayGroup(sbitsAggregateFunc* aggFunc, sbitsSchema* schema, void* recordBuffer, const void* lastRecord) {
    // Put day in record
    uint32_t day = dayGroup(lastRecord);
    memcpy((int8_t*)recordBuffer + getColOffsetFromSchema(schema, aggFunc->colNum), &day, sizeof(uint32_t));
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
