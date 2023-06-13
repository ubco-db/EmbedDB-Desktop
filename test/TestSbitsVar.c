#include <math.h>
#include <string.h>

#include "../src/sbits/sbits.h"
#include "../src/sbits/utilityFunctions.h"
#include "unity.h"

sbitsState *state;
int32_t numRecords = 500000;
int32_t testRecords = 500000;

void setUp(void) {
    // Initialize sbits State
    state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 12;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->startAddress = 0;
    state->endAddress = 1000 * state->pageSize;
    state->varAddressStart = 0;
    state->varAddressEnd = 1000 * state->pageSize;
    state->eraseSizeInPages = 4;
    state->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX | SBITS_USE_VDATA;
    state->bitmapSize = 1;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;

    // Test initialization was correct
    int result = sbitsInit(state, 1);

    TEST_ASSERT_EQUAL_INT_MESSAGE(0, result, "There was an error with sbitsInit. Sbits was not initalized correctly.");

    TEST_ASSERT_EQUAL_INT8_MESSAGE(4, state->keySize, "Key size was changed during init");
    TEST_ASSERT_EQUAL_INT8_MESSAGE(12, state->dataSize, "Data size was changed during init");
    TEST_ASSERT_EQUAL_INT8_MESSAGE(state->keySize + state->dataSize, state->recordSize, "Record size is not 4 (key) + 12 (data) = 16");

    TEST_ASSERT_NOT_NULL_MESSAGE(state->file, "sbitsInit did not open the data file");
    TEST_ASSERT_NOT_NULL_MESSAGE(state->indexFile, "sbitsInit did not open the index file");
}

void iteratorReturnsCorrectVarRecords(void) {
    resetStats(state);

    // Insert records
    uint8_t dataLen = 15;
    char vardata[15] = "Testing 000...";
    for (uint32_t key = 0; key < numRecords; key++) {
        uint32_t data = key % 100;
        void *varData = NULL;
        uint32_t length = 0;
        if (key % 10 == 0) {
            length = 15;
            vardata[10] = (char)(key % 10) + '0';
            vardata[9] = (char)((key / 10) % 10) + '0';
            vardata[8] = (char)((key / 100) % 10) + '0';
            varData = malloc(length);
            memcpy(varData, vardata, length);
        }

        sbitsPutVar(state, &key, &data, varData, length);

        if (varData != NULL) {
            free(varData);
            varData = NULL;
        }
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
    uint8_t varBufSize = 4;
    void *varBuf = malloc(varBufSize);
    sbitsVarDataStream *varStream;
    while (sbitsNextVar(state, &it, &key, &data, &varStream)) {
        numRecordsRead++;
        TEST_ASSERT_GREATER_OR_EQUAL_UINT32_MESSAGE(minKey, key, "Key is not in range of query");
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(key % 100, data, "Record contains the wrong data");
        TEST_ASSERT_GREATER_OR_EQUAL_UINT32_MESSAGE(minData, data, "Data is not in range of query");
        TEST_ASSERT_LESS_OR_EQUAL_UINT32_MESSAGE(maxData, data, "Data is not in range of query");
        if (key % 10 == 0) {
            TEST_ASSERT_NOT_NULL_MESSAGE(varStream, "Record should contain vardata and return a datastream");
        } else {
            TEST_ASSERT_NULL_MESSAGE(varStream, "Record should not contain vardata or return a datastream");
        }

        if (varStream != NULL) {
            int8_t count = 0;
            char reconstructedData[15];
            uint32_t bytesRead, totalBytes = 0;
            while ((bytesRead = sbitsVarDataStreamRead(state, varStream, varBuf, varBufSize)) > 0) {
                TEST_ASSERT_LESS_OR_EQUAL_UINT8_MESSAGE(dataLen, totalBytes + bytesRead, "Datastream is returning too much data");
                memcpy(reconstructedData + totalBytes, varBuf, bytesRead);
                totalBytes += bytesRead;
            }

            char expected[15] = "Testing 000...";
            expected[10] = (char)(key % 10) + '0';
            expected[9] = (char)((key / 10) % 10) + '0';
            expected[8] = (char)((key / 100) % 10) + '0';
            TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(expected, reconstructedData, dataLen, "Datastream did not return the correct data");

            free(varStream);
            varStream = NULL;
        }
    }

    free(varBuf);

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

void tearDown(void) {
    sbitsClose(state);
    free(state);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(iteratorReturnsCorrectVarRecords);
    return UNITY_END();
}
