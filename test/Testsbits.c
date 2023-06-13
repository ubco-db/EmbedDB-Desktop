#include <math.h>
#include <string.h>

#include "../src/sbits/sbits.h"
#include "../src/sbits/utilityFunctions.h"
#include "unity.h"

sbitsState *state;
int32_t numRecords = 1000;
int32_t testRecords = 1000;

void setUp(void) {
    state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->startAddress = 0;
    state->endAddress = 1000 * state->pageSize;
    state->eraseSizeInPages = 4;
    state->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX;
    state->bitmapSize = 1;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;

    // printf("Hello King Julian");

    int result = sbitsInit(state, 1);
    // TEST_ASSERT_EQUAL_INT_MESSAGE(0, result, "There was an error with sbitsInit. Sbits was not initalized correctly.");

    // TEST_ASSERT_EQUAL_INT8_MESSAGE(4, state->keySize, "Key size was changed during init");
    // TEST_ASSERT_EQUAL_INT8_MESSAGE(12, state->dataSize, "Data size was changed during init");
    // TEST_ASSERT_EQUAL_INT8_MESSAGE(state->keySize + state->dataSize, state->recordSize, "Record size is not 4 (key) + 12 (data) = 16");

    // TEST_ASSERT_NOT_NULL_MESSAGE(state->file, "sbitsInit did not open the data file");
    // TEST_ASSERT_NOT_NULL_MESSAGE(state->indexFile, "sbitsInit did not open the index file");
    resetStats(state);
}

void tearDown(void) {
    sbitsClose(state);
    free(state);
}

void iteratorReturnsCorrectRecords(void) {
    for (uint32_t key = 0; key < numRecords; key++) {
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
    for (int i = 0; i < numRecords; i++) {
        if (it.minKey != NULL && i < *(uint32_t *)it.minKey) continue;
        if (it.maxKey != NULL && i > *(uint32_t *)it.maxKey) continue;
        if (it.minData != NULL && i % 100 < *(uint32_t *)it.minData) continue;
        if (it.maxData != NULL && i % 100 > *(uint32_t *)it.maxData) continue;
        expectedNum++;
    }
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(expectedNum, numRecordsRead, "Iterator did not read the correct number of records");

    // Check that not all pages were read
    TEST_ASSERT_LESS_THAN_UINT32_MESSAGE(numRecords / state->maxRecordsPerPage + 1, state->numReads - numPageReads, "Iterator made too many reads");
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(iteratorReturnsCorrectRecords);
    return UNITY_END();
}
