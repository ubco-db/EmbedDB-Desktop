#include <math.h>
#include <string.h>

#include "../src/sbits/sbits.h"
#include "../src/sbits/utilityFunctions.h"
#include "unity.h"

sbitsState *state;

void setUp(void) {
    state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 8;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->startAddress = 0;
    state->endAddress = 93 * state->pageSize;
    state->eraseSizeInPages = 4;
    state->bitmapSize = 0;
    state->parameters = SBITS_RESET_DATA;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int64Comparator;
    int8_t result = sbitsInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "SBITS did not initialize correctly.");
}

void initalizeSbitsFromFile(void) {
    state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 8;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->startAddress = 0;
    state->endAddress = 93 * state->pageSize;
    state->eraseSizeInPages = 4;
    state->bitmapSize = 0;
    state->parameters = 0;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int64Comparator;
    int8_t result = sbitsInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "SBITS did not initialize correctly.");
}

void insertRecordsLinear(int32_t startingKey, int32_t startingData, int32_t numRecords) {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    *((int32_t *)data) = startingKey;
    *((int32_t *)(data + 4)) = startingData;
    for (int i = 0; i < numRecords; i++) {
        *((int32_t *)data) += 1;
        *((int32_t *)(data + 4)) += 1;
        int8_t result = sbitsPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "sbitsPut did not correctly insert data (returned non-zero code)");
    }
    free(data);
}

void sbits_parameters_initializes_from_data_file_with_twenty_seven_pages_correctly() {
    insertRecordsLinear(9, 20230614, 1135);
    sbitsClose(state);
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(10, state->minKey, "SBITS minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(27, state->nextPageId, "SBITS nextPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(27, state->nextPageWriteId, "SBITS nextPageWriteId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->firstDataPage, "SBITS firstDataPage is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->firstDataPageId, "SBITS firstDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(27, state->erasedEndPage, "SBITS erasedEndPage is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, state->wrappedMemory, "SBITS wrappedMemory is not correctly identified after reload from data file.");
}

/* The setup function allocates 93 pages, so check to make sure it initalizes correctly when it is full */
void sbits_parameters_initializes_from_data_file_with_ninety_three_pages_correctly() {
    insertRecordsLinear(3456, 2548, 3907);
    sbitsClose(state);
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(3457, state->minKey, "SBITS minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(93, state->nextPageId, "SBITS nextPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(93, state->nextPageWriteId, "SBITS nextPageWriteId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->firstDataPage, "SBITS firstDataPage is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->firstDataPageId, "SBITS firstDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(93, state->erasedEndPage, "SBITS erasedEndPage is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, state->wrappedMemory, "SBITS wrappedMemory is not correctly identified after reload from data file.");
}

void sbits_parameters_initializes_from_data_file_with_ninety_four_one_pages_correctly() {
    insertRecordsLinear(1645, 2548, 3949);
    sbitsClose(state);
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(1688, state->minKey, "SBITS minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(94, state->nextPageId, "SBITS nextPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextPageWriteId, "SBITS nextPageWriteId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->firstDataPage, "SBITS firstDataPage is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->firstDataPageId, "SBITS firstDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->erasedEndPage, "SBITS erasedEndPage is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_INT8_MESSAGE(1, state->wrappedMemory, "SBITS wrappedMemory is not32 correctly identified after reload from data file.");
}

void sbits_parameters_initializes_correctly_from_data_file_with_four_hundred_seventeen_previous_page_inserts() {
    insertRecordsLinear(2000, 11205, 17515);
    sbitsClose(state);
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(15609, state->minKey, "SBITS minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(417, state->nextPageId, "SBITS nextPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(45, state->nextPageWriteId, "SBITS nextPageWriteId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(45, state->firstDataPage, "SBITS firstDataPage is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(324, state->firstDataPageId, "SBITS firstDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(44, state->erasedEndPage, "SBITS erasedEndPage is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_INT8_MESSAGE(1, state->wrappedMemory, "SBITS wrappedMemory is not32 correctly identified after reload from data file.");
}

void sbits_inserts_correctly_into_data_file_after_reload() {
    insertRecordsLinear(0, 0, 1975);
    sbitsClose(state);
    initalizeSbitsFromFile();
    int32_t key = 1974;
    int32_t data = 1974;
    int8_t insertResult = sbitsPut(state, &key, &data);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(1, insertResult, "SBITS inserted a duplicate key.");
    insertRecordsLinear(1974, 8946, 43);
    int8_t *recordBuffer = (int8_t *)malloc(state->dataSize);
    key = 1975;
    int8_t getResult = sbitsGet(state, &key, recordBuffer);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, "SBITS get encountered an error fetching the data for key 1975.");
    if(getResult != 0) {
        printf("There was an error getting the data.\n");
    }
    TEST_ASSERT_EQUAL_INT64_MESSAGE(8947, *((int32_t *)recordBuffer), "SBITS get did not return correct data after inserting records after reload.");
}

void tearDown(void) {
    sbitsClose(state);
    free(state);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(sbits_parameters_initializes_from_data_file_with_twenty_seven_pages_correctly);
    RUN_TEST(sbits_parameters_initializes_from_data_file_with_ninety_three_pages_correctly);
    RUN_TEST(sbits_parameters_initializes_from_data_file_with_ninety_four_one_pages_correctly);
    RUN_TEST(sbits_parameters_initializes_correctly_from_data_file_with_four_hundred_seventeen_previous_page_inserts);
    RUN_TEST(sbits_inserts_correctly_into_data_file_after_reload);
    return UNITY_END();
}
