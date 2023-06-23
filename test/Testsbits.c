#include <math.h>
#include <string.h>

#include "../src/sbits/sbits.h"
#include "../src/sbits/utilityFunctions.h"
#include "unity.h"

sbitsState *state;

void setUp(void) {
    state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->numDataPages = 1000;
    state->parameters = 0;
    state->eraseSizeInPages = 4;
    state->fileInterface = getFileInterface();
    char dataPath[] = "build/artifacts/dataFile.bin", indexPath[] = "build/artifacts/indexFile.bin", varPath[] = "build/artifacts/varFile.bin";
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);
    state->varFile = setupFile(varPath);
    state->bitmapSize = 0;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int result = sbitsInit(state, 1);
}

void sbits_initial_configuration_is_correct() {
    TEST_ASSERT_NOT_NULL_MESSAGE(state->dataFile, "SBITS file was not initialized correctly.");
    TEST_ASSERT_NULL_MESSAGE(state->varFile, "SBITS varFile was intialized for non-variable data.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextPageId, "SBITS nextPageId was not initialized correctly.");
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, state->wrappedMemory, "SBITS did not initalized wrappedMemory correctly.");
    TEST_ASSERT_EQUAL_INT8_MESSAGE(6, state->headerSize, "SBITS headerSize was not initialized correctly.");
    TEST_ASSERT_EQUAL_INT64_MESSAGE(0, state->minKey, "SBITS minKey was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(UINT32_MAX, state->bufferedPageId, "SBITS bufferedPageId was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(UINT32_MAX, state->bufferedIndexPageId, "SBITS bufferedIndexPageId was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(UINT32_MAX, state->bufferedVarPage, "SBITS bufferedVarPage was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT16_MESSAGE(63, state->maxRecordsPerPage, "SBITS maxRecordsPerPage was not initialized correctly.");
    TEST_ASSERT_EQUAL_INT32_MESSAGE(63, state->maxError, "SBITS maxError was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1000, state->numDataPages, "SBITS numDataPages was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "SBITS firstDataPage was not initialized correctly.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->avgKeyDiff, "SBITS avgKeyDiff was not initialized correctly.");
    TEST_ASSERT_NOT_NULL_MESSAGE(state->spl, "SBITS spline was not initialized correctly.");
}

void sbits_put_inserts_single_record_correctly() {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    *((int32_t *)data) = 15648;
    *((int32_t *)(data + 4)) = 27335;
    int8_t result = sbitsPut(state, data, (void *)(data + 4));
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "sbitsPut did not correctly insert data (returned non-zero code)");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(15648, state->minKey, "sbitsPut did not update minimim key on first insert.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextPageId, "sbitsPut incremented next page to write and it should not have.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, SBITS_GET_COUNT(state->buffer), "sbitsPut did not increment count in buffer correctly.");
    int32_t *sbitsPutResultKey = malloc(sizeof(int32_t));
    int32_t *sbitsPutResultData = malloc(sizeof(int32_t));
    memcpy(sbitsPutResultKey, (int8_t *)state->buffer + 6, 4);
    memcpy(sbitsPutResultData, (int8_t *)state->buffer + 10, 4);
    TEST_ASSERT_EQUAL_INT32_MESSAGE(15648, *sbitsPutResultKey, "sbitsPut did not put correct key value in buffer.");
    TEST_ASSERT_EQUAL_INT32_MESSAGE(27335, *sbitsPutResultData, "sbitsPut did not put correct data value in buffer.");
    free(sbitsPutResultKey);
    free(sbitsPutResultData);
    free(data);
}

void sbits_put_inserts_eleven_records_correctly() {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    int32_t *sbitsPutResultKey = malloc(sizeof(int32_t));
    int32_t *sbitsPutResultData = malloc(sizeof(int32_t));
    *((int32_t *)data) = 16321;
    *((int32_t *)(data + 4)) = 28361;
    for (int i = 0; i < 11; i++) {
        *((int32_t *)data) += i;
        *((int32_t *)(data + 4)) %= (i + 1);
        int8_t result = sbitsPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "sbitsPut did not correctly insert data (returned non-zero code)");
        memcpy(sbitsPutResultKey, (int8_t *)state->buffer + 6 + (i * 8), 4);
        memcpy(sbitsPutResultData, (int8_t *)state->buffer + 10 + (i * 8), 4);
        TEST_ASSERT_EQUAL_INT32_MESSAGE(*((int32_t *)data), *sbitsPutResultKey, "sbitsPut did not put correct key value in buffer.");
        TEST_ASSERT_EQUAL_INT32_MESSAGE(*((int32_t *)(data + 4)), *sbitsPutResultData, "sbitsPut did not put correct data value in buffer.");
    }
    TEST_ASSERT_EQUAL_INT64_MESSAGE(16321, state->minKey, "sbitsPut did not update minimim key on first insert.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextPageId, "sbitsPut incremented next page to write and it should not have.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(11, SBITS_GET_COUNT(state->buffer), "sbitsPut did not increment count in buffer correctly.");
    free(sbitsPutResultKey);
    free(sbitsPutResultData);
    free(data);
}

void sbits_put_inserts_one_page_of_records_correctly() {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    int32_t *sbitsPutResultKey = malloc(sizeof(int32_t));
    int32_t *sbitsPutResultData = malloc(sizeof(int32_t));
    *((int32_t *)data) = 100;
    *((int32_t *)(data + 4)) = 724;
    for (int i = 0; i < 63; i++) {
        *((int32_t *)data) += i;
        *((int32_t *)(data + 4)) %= (i + 1);
        int8_t result = sbitsPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "sbitsPut did not correctly insert data (returned non-zero code)");
        memcpy(sbitsPutResultKey, (int8_t *)state->buffer + 6 + (i * 8), 4);
        memcpy(sbitsPutResultData, (int8_t *)state->buffer + 10 + (i * 8), 4);
        TEST_ASSERT_EQUAL_INT32_MESSAGE(*((int32_t *)data), *sbitsPutResultKey, "sbitsPut did not put correct key value in buffer.");
        TEST_ASSERT_EQUAL_INT32_MESSAGE(*((int32_t *)(data + 4)), *sbitsPutResultData, "sbitsPut did not put correct data value in buffer.");
    }
    TEST_ASSERT_EQUAL_INT64_MESSAGE(100, state->minKey, "sbitsPut did not update minimim key on first insert.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextPageId, "sbitsPut incremented next page to write and it should not have.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(63, SBITS_GET_COUNT(state->buffer), "sbitsPut did not increment count in buffer correctly.");
    free(sbitsPutResultKey);
    free(sbitsPutResultData);
    free(data);
}

void sbits_put_inserts_one_more_than_one_page_of_records_correctly() {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    int32_t *sbitsPutResultKey = malloc(sizeof(int32_t));
    int32_t *sbitsPutResultData = malloc(sizeof(int32_t));
    *((int32_t *)data) = 4444444;
    *((int32_t *)(data + 4)) = 96875;
    for (int i = 0; i < 64; i++) {
        *((int32_t *)data) += i;
        *((int32_t *)(data + 4)) %= (i + 1);
        int8_t result = sbitsPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "sbitsPut did not correctly insert data (returned non-zero code)");
    }
    TEST_ASSERT_EQUAL_INT64_MESSAGE(4444444, state->minKey, "sbitsPut did not update minimim key on first insert.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextPageId, "sbitsPut did not move to next page after writing the first page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, SBITS_GET_COUNT(state->buffer), "sbitsPut did not reset buffer count to correct value after writing the page");
    free(sbitsPutResultKey);
    free(sbitsPutResultData);
    free(data);
}

void iteratorReturnsCorrectRecords(void) {
    int32_t numRecordsToInsert = 1000;
    for (uint32_t key = 0; key < numRecordsToInsert; key++) {
        uint32_t data = key % 100;
        sbitsPut(state, &key, &data);
    }
    sbitsFlush(state);

    // Query records using an iterator
    sbitsIterator it;
    uint32_t minData = 23, maxData = 38, minKey = 32;
    it.minData = &minData;
    it.maxData = &maxData;
    it.minKey = &minKey;
    it.maxKey = NULL;
    sbitsInitIterator(state, &it);

    uint32_t numRecordsRead = 0, numPageReads = state->numReads;
    uint32_t key, data;
    int32_t numDataIncorrect = 0;
    while (sbitsNext(state, &it, &key, &data)) {
        numRecordsRead++;
        TEST_ASSERT_GREATER_OR_EQUAL_UINT32_MESSAGE(minKey, key, "Key is not in range of query");
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(key % 100, data, "Record contains the wrong data");
        TEST_ASSERT_GREATER_OR_EQUAL_UINT32_MESSAGE(minData, data, "Data is not in range of query");
        TEST_ASSERT_LESS_OR_EQUAL_UINT32_MESSAGE(maxData, data, "Data is not in range of query");
    }

    // Check that the correct number of records were read
    uint32_t expectedNum = 0;
    for (int i = 0; i < numRecordsToInsert; i++) {
        if (it.minKey != NULL && i < *(uint32_t *)it.minKey) continue;
        if (it.maxKey != NULL && i > *(uint32_t *)it.maxKey) continue;
        if (it.minData != NULL && i % 100 < *(uint32_t *)it.minData) continue;
        if (it.maxData != NULL && i % 100 > *(uint32_t *)it.maxData) continue;
        expectedNum++;
    }
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedNum, numRecordsRead, "Iterator did not read the correct number of records");
}

void tearDown(void) {
    sbitsClose(state);
    tearDownFile(state->dataFile);
    free(state->buffer);
    free(state->fileInterface);
    free(state);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(sbits_initial_configuration_is_correct);
    RUN_TEST(sbits_put_inserts_single_record_correctly);
    RUN_TEST(sbits_put_inserts_eleven_records_correctly);
    RUN_TEST(sbits_put_inserts_one_page_of_records_correctly);
    RUN_TEST(sbits_put_inserts_one_more_than_one_page_of_records_correctly);
    RUN_TEST(iteratorReturnsCorrectRecords);
    return UNITY_END();
}
