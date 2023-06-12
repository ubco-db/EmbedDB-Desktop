#include <math.h>
#include <string.h>

#include "../lib/unity/unity.h"
#include "../src/sbits.h"

/* A bitmap with 8 buckets (bits). Range 0 to 100. */
void updateBitmapInt8(void *data, void *bm) {
    // Note: Assuming int key is right at the start of the data record
    int32_t val = *((int16_t *)data);
    uint8_t *bmval = (uint8_t *)bm;

    if (val < 10)
        *bmval = *bmval | 128;
    else if (val < 20)
        *bmval = *bmval | 64;
    else if (val < 30)
        *bmval = *bmval | 32;
    else if (val < 40)
        *bmval = *bmval | 16;
    else if (val < 50)
        *bmval = *bmval | 8;
    else if (val < 60)
        *bmval = *bmval | 4;
    else if (val < 100)
        *bmval = *bmval | 2;
    else
        *bmval = *bmval | 1;
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. Build bitmap based on min and
 * max value. */
void buildBitmapInt8FromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint8_t *)bm = 255; /* Everything */
    } else {
        uint8_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateBitmapInt8(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint8_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt8(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = ~(maxMap - 1);
            if (min == NULL) {
                *(uint8_t *)bm = maxMap;
                return;
            }
        }
        *(uint8_t *)bm = minMap & maxMap;
    }
}

int8_t inBitmapInt8(void *data, void *bm) {
    uint8_t *bmval = (uint8_t *)bm;

    uint8_t tmpbm = 0;
    updateBitmapInt8(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/* A 16-bit bitmap on a 32-bit int value */
void updateBitmapInt16(void *data, void *bm) {
    int32_t val = *((int32_t *)data);
    uint16_t *bmval = (uint16_t *)bm;

    /* Using a demo range of 0 to 100 */
    // int16_t stepSize = 100 / 15;
    int16_t stepSize = 450 / 15;  // Temperature data in F. Scaled by 10. */
    int16_t minBase = 320;
    int32_t current = minBase;
    uint16_t num = 32768;
    while (val > current) {
        current += stepSize;
        num = num / 2;
    }
    if (num == 0)
        num = 1; /* Always set last bit if value bigger than largest cutoff */
    *bmval = *bmval | num;
}

int8_t inBitmapInt16(void *data, void *bm) {
    uint16_t *bmval = (uint16_t *)bm;

    uint16_t tmpbm = 0;
    updateBitmapInt16(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/**
 * @brief	Builds 16-bit bitmap from (min, max) range.
 * @param	state	SBITS state structure
 * @param	min		minimum value (may be NULL)
 * @param	max		maximum value (may be NULL)
 * @param	bm		bitmap created
 */
void buildBitmapInt16FromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint16_t *)bm = 65535; /* Everything */
        return;
    } else {
        uint16_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateBitmapInt8(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = minMap | (minMap - 1);
            if (max == NULL) {
                *(uint16_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateBitmapInt8(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = ~(maxMap - 1);
            if (min == NULL) {
                *(uint16_t *)bm = maxMap;
                return;
            }
        }
        *(uint16_t *)bm = minMap & maxMap;
    }
}

/* A 64-bit bitmap on a 32-bit int value */
void updateBitmapInt64(void *data, void *bm) {
    int32_t val = *((int32_t *)data);

    int16_t stepSize = 10;  // Temperature data in F. Scaled by 10. */
    int32_t current = 320;
    int8_t bmsize = 63;
    int8_t count = 0;

    while (val > current && count < 63) {
        current += stepSize;
        count++;
    }
    uint8_t b = 128;
    int8_t offset = count / 8;
    b = b >> (count & 7);

    *((char *)((char *)bm + offset)) = *((char *)((char *)bm + offset)) | b;
}

int8_t inBitmapInt64(void *data, void *bm) {
    uint64_t *bmval = (uint64_t *)bm;

    uint64_t tmpbm = 0;
    updateBitmapInt64(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

int8_t int32Comparator(void *a, void *b) {
    int32_t result = *((int32_t *)a) - *((int32_t *)b);
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}

void setUp(void) {
}

void tearDown(void) {
}

void sbtisInitShouldInitState(void) {
    sbitsState *state = (sbitsState *)malloc(sizeof(sbitsState));
    int8_t M = 4;
    int32_t numRecords = 500000;   // default values
    int32_t testRecords = 500000;  // default values

    state->recordSize = 16;
    state->keySize = 4;
    state->dataSize = 12;
    state->pageSize = 512;
    state->bitmapSize = 0;
    state->bufferSizeInBlocks = M;
    state->buffer =
        malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    int8_t *recordBuffer = (int8_t *)malloc(state->recordSize);

    /* Address level parameters */
    state->startAddress = 0;
    state->endAddress =
        state->pageSize * numRecords /
        10; /* Modify this value lower to test wrap around */
    state->eraseSizeInPages = 4;
    // state->parameters = SBITS_USE_MAX_MIN | SBITS_USE_BMAP |
    // SBITS_USE_INDEX;
    state->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX;
    // state->parameters =  0;
    if (SBITS_USING_INDEX(state->parameters) == 1)
        state->endAddress += state->pageSize * (state->eraseSizeInPages * 2);
    if (SBITS_USING_BMAP(state->parameters))
        state->bitmapSize = 8;

    /* Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt16;
    state->updateBitmap = updateBitmapInt16;
    state->inBitmap = inBitmapInt64;
    state->updateBitmap = updateBitmapInt64;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int result = sbitsInit(state, 1);
    TEST_ASSERT_EQUAL_INT_MESSAGE(0, result, "There was an error with sbitsInit. Sbits was not initalized correctly.");
    free(state);
}

void iteratorReturnsCorrectRecords(void) {
    uint32_t numRecords = 1000;

    // Setup sbitsState
    sbitsState *state = malloc(sizeof(sbitsState));
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
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, sbitsInit(state, 0), "Failed to initialize sbitsState");
    resetStats(state);

    // Insert records
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

    // Free resources
    free(state->buffer);
    free(state);
}

void iteratorReturnsCorrectVarRecords(void) {
    uint32_t numRecords = 1000;

    // Setup sbitsState
    sbitsState *state = malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 4;
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
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, sbitsInit(state, 0), "Failed to initialize sbitsState");
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

    // Free resources
    free(state->buffer);
    free(state);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(sbtisInitShouldInitState);
    RUN_TEST(iteratorReturnsCorrectRecords);
    RUN_TEST(iteratorReturnsCorrectVarRecords);
    return UNITY_END();
}
