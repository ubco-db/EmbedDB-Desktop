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
    state->fileInterface = getFileInterface();
    char dataPath[] = "build/artifacts/dataFile.bin";
    state->dataFile = setupFile(dataPath);
    state->numDataPages = 93;
    state->eraseSizeInPages = 4;
    state->parameters = SBITS_RESET_DATA;
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
    state->fileInterface = getFileInterface();
    char dataPath[] = "build/artifacts/dataFile.bin";
    state->dataFile = setupFile(dataPath);
    state->numDataPages = 93;
    state->eraseSizeInPages = 4;
    state->parameters = 0;
    state->compareKey = int32Comparator;
    state->compareData = int64Comparator;
    int8_t result = sbitsInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "SBITS did not initialize correctly.");
}

void tearDown(void) {
    free(state->buffer);
    sbitsClose(state);
    tearDownFile(state->dataFile);
    free(state->fileInterface);
    free(state);
}

void insertRecordsLinearly(int32_t startingKey, int64_t startingData, int32_t numRecords) {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    *((int32_t *)data) = startingKey;
    *((int64_t *)(data + 4)) = startingData;
    for (int i = 0; i < numRecords; i++) {
        *((int32_t *)data) += 1;
        *((int64_t *)(data + 4)) += 1;
        int8_t result = sbitsPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "sbitsPut did not correctly insert data (returned non-zero code)");
    }
    free(data);
}

void insertRecordsParabolic(int32_t startingKey, int64_t startingData, int32_t numRecords) {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    *((int32_t *)data) = startingKey;
    *((int64_t *)(data + 4)) = startingData;
    for (int i = 0; i < numRecords; i++) {
        *((int32_t *)data) += i;
        *((int64_t *)(data + 4)) += 1;
        int8_t result = sbitsPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "sbitsPut did not correctly insert data (returned non-zero code)");
    }
    free(data);
}

int queryRecordsLinearly(sbitsState *state, uint32_t numberOfRecords, int32_t startingKey, int64_t startingData) {
    int64_t *result = (int64_t *)malloc(state->recordSize);
    int32_t key = startingKey;
    int64_t data = startingData;
    for (int32_t i = 0; i < numberOfRecords; i++) {
        key = startingKey += 1;
        data += 1;
        int8_t getStatus = sbitsGet(state, &key, (void *)result);
        if (getStatus != 0) {
            printf("ERROR: Failed to find: %i\n", key);
            return 1;
        }
        if (*((int64_t *)result) != data) {
            printf("ERROR: Wrong data for: %i\n", key);
            printf("Key: %i Data: %lu\n", key, *((int64_t *)result));
            free(result);
            return 1;
        }
    }
    free(result);
    return 0;
}

void sbits_parameters_initializes_from_data_file_with_twenty_seven_pages_correctly() {
    insertRecordsLinearly(9, 20230614, 1135);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(10, state->minKey, "SBITS minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(27, state->nextDataPageId, "SBITS nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "SBITS minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(66, state->numAvailDataPages, "SBITS numAvailDataPages is not correctly initialized.");
}

/* The setup function allocates 93 pages, so check to make sure it initalizes correctly when it is full */
void sbits_parameters_initializes_from_data_file_with_ninety_three_pages_correctly() {
    insertRecordsLinearly(3456, 2548, 3907);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(3457, state->minKey, "SBITS minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(93, state->nextDataPageId, "SBITS nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "SBITS minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailDataPages, "SBITS numAvailDataPages is not correctly initialized.");
}

void sbits_parameters_initializes_from_data_file_with_ninety_four_pages_correctly() {
    insertRecordsLinearly(1645, 2548, 3949);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(1688, state->minKey, "SBITS minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(94, state->nextDataPageId, "SBITS nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->minDataPageId, "SBITS minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailDataPages, "SBITS numAvailDataPages is not correctly initialized.");
}

void sbits_parameters_initializes_correctly_from_data_file_with_four_hundred_seventeen_previous_page_inserts() {
    insertRecordsLinearly(2000, 11205, 17515);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(15609, state->minKey, "SBITS minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(417, state->nextDataPageId, "SBITS nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(324, state->minDataPageId, "SBITS minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailDataPages, "SBITS numAvailDataPages is not correctly initialized.");
}

void sbits_parameters_initializes_correctly_from_data_file_with_no_data() {
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(UINT32_MAX, state->minKey, "SBITS minkey is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextDataPageId, "SBITS nextDataPageId is not correctly identified after reload from data file.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->minDataPageId, "SBITS minDataPageId was not correctly identified.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(93, state->numAvailDataPages, "SBITS numAvailDataPages is not ");
}

void sbits_inserts_correctly_into_data_file_after_reload() {
    insertRecordsLinearly(1000, 5600, 3655);
    tearDown();
    initalizeSbitsFromFile();
    insertRecordsLinearly(4654, 10, 43);
    int8_t *recordBuffer = (int8_t *)malloc(state->dataSize);
    int32_t key = 1001;
    int64_t data = 5601;
    char keyMessage[80];
    char dataMessage[100];
    /* Records inserted before reload */
    for (int i = 0; i < 3654; i++) {
        int8_t getResult = sbitsGet(state, &key, recordBuffer);
        snprintf(keyMessage, 80, "SBITS get encountered an error fetching the data for key %i.", key);
        snprintf(dataMessage, 100, "SBITS get did not return correct data for a record inserted before reloading (key %i).", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, keyMessage);
        TEST_ASSERT_EQUAL_INT64_MESSAGE(data, *((int64_t *)recordBuffer), dataMessage);
        key++;
        data++;
    }
    /* Records inserted after reload */
    data = 11;
    for (int i = 0; i < 42; i++) {
        int8_t getResult = sbitsGet(state, &key, recordBuffer);
        snprintf(keyMessage, 80, "SBITS get encountered an error fetching the data for key %i.", key);
        snprintf(dataMessage, 100, "SBITS get did not return correct data for a record inserted after reloading (key %i).", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, keyMessage);
        TEST_ASSERT_EQUAL_INT64_MESSAGE(data, *((int64_t *)recordBuffer), dataMessage);
        key++;
        data++;
    }
    free(recordBuffer);
}

void sbits_correctly_gets_records_after_reload_with_wrapped_data() {
    insertRecordsLinearly(0, 0, 13758);
    sbitsFlush(state);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(9871, state->minKey, "SBITS minkey is not the correct value after reloading.");
    int8_t *recordBuffer = (int8_t *)malloc(state->dataSize);
    int32_t key = 9871;
    int64_t data = 9871;
    char keyMessage[80];
    char dataMessage[100];
    /* Records inserted before reload */
    for (int i = 0; i < 3888; i++) {
        int8_t getResult = sbitsGet(state, &key, recordBuffer);
        snprintf(keyMessage, 80, "SBITS get encountered an error fetching the data for key %i.", key);
        snprintf(dataMessage, 100, "SBITS get did not return correct data for a record inserted before reloading (key %i).", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, keyMessage);
        TEST_ASSERT_EQUAL_INT64_MESSAGE(data, *((int64_t *)recordBuffer), dataMessage);
        key++;
        data++;
    }
    free(recordBuffer);
}

void sbits_prevents_duplicate_inserts_after_reload() {
    insertRecordsLinearly(0, 8751, 1975);
    tearDown();
    initalizeSbitsFromFile();
    int32_t key = 1974;
    int64_t data = 1974;
    int8_t insertResult = sbitsPut(state, &key, &data);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(1, insertResult, "SBITS inserted a duplicate key.");
}

void sbits_queries_correctly_with_non_liner_data_after_reload() {
    insertRecordsParabolic(1000, 367, 4495);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(174166, state->minKey, "SBITS minkey is not the correct value after reloading.");
    int8_t *recordBuffer = (int8_t *)malloc(state->dataSize);
    int32_t key = 174166;
    int64_t data = 956;
    char keyMessage[80];
    char dataMessage[100];
    /* Records inserted before reload */
    for (int i = 174166; i < 4494; i++) {
        int8_t getResult = sbitsGet(state, &key, recordBuffer);
        snprintf(keyMessage, 80, "SBITS get encountered an error fetching the data for key %i.", key);
        snprintf(dataMessage, 100, "SBITS get did not return correct data for a record inserted before reloading (key %i).", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, keyMessage);
        TEST_ASSERT_EQUAL_INT64_MESSAGE(data, *((int64_t *)recordBuffer), dataMessage);
        key += i;
        data += i;
    }
    free(recordBuffer);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(sbits_parameters_initializes_from_data_file_with_twenty_seven_pages_correctly);
    RUN_TEST(sbits_parameters_initializes_from_data_file_with_ninety_three_pages_correctly);
    RUN_TEST(sbits_parameters_initializes_from_data_file_with_ninety_four_pages_correctly);
    RUN_TEST(sbits_parameters_initializes_correctly_from_data_file_with_four_hundred_seventeen_previous_page_inserts);
    RUN_TEST(sbits_inserts_correctly_into_data_file_after_reload);
    RUN_TEST(sbits_correctly_gets_records_after_reload_with_wrapped_data);
    RUN_TEST(sbits_prevents_duplicate_inserts_after_reload);
    RUN_TEST(sbits_queries_correctly_with_non_liner_data_after_reload);
    RUN_TEST(sbits_parameters_initializes_correctly_from_data_file_with_no_data);
    return UNITY_END();
}
