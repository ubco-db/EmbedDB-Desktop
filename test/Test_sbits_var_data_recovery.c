#include <math.h>
#include <string.h>

#include "../src/sbits/sbits.h"
#include "../src/sbits/utilityFunctions.h"
#include "unity.h"

sbitsState *state;

void setUp(void) {
    // Initialize sbits State
    state = malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->numDataPages = 65;
    state->numVarPages = 75;
    state->eraseSizeInPages = 4;
    char dataPath[] = "build/artifacts/dataFile.bin", indexPath[] = "build/artifacts/indexFile.bin", varPath[] = "build/artifacts/varFile.bin";
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->varFile = setupFile(varPath);
    state->parameters = SBITS_USE_VDATA | SBITS_RESET_DATA;
    state->bitmapSize = 0;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = sbitsInit(state, 1);
    resetStats(state);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "SBITS did not initialize correctly.");
}

void initalizeSbitsFromFile(void) {
    // Initialize sbits State
    state = malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->numDataPages = 65;
    state->numVarPages = 75;
    state->eraseSizeInPages = 4;
    char dataPath[] = "build/artifacts/dataFile.bin", indexPath[] = "build/artifacts/indexFile.bin", varPath[] = "build/artifacts/varFile.bin";
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->varFile = setupFile(varPath);
    state->parameters = SBITS_USE_VDATA;
    state->bitmapSize = 0;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = sbitsInit(state, 1);
    resetStats(state);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "SBITS did not initialize correctly.");
}

void tearDown(void) {
    sbitsClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->varFile);
    free(state->buffer);
    free(state->fileInterface);
    free(state);
}

void insertRecords(int32_t numberOfRecords, int32_t startingKey, int32_t startingData) {
    int32_t key = startingKey;
    int32_t data = startingData;
    int8_t *recordBuffer = (int8_t *)calloc(1, state->recordSize);
    *((int32_t *)recordBuffer) = key;
    *((int32_t *)(recordBuffer + state->keySize)) = startingData;
    char variableData[13] = "Hello World!";
    for (int32_t i = 0; i < numberOfRecords; i++) {
        *((int32_t *)recordBuffer) += 1;
        *((int32_t *)(recordBuffer + state->keySize)) += 1;
        int8_t insertResult = sbitsPutVar(state, recordBuffer, (int8_t *)recordBuffer + state->keySize, variableData, 13);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "SBITS failed to insert data.");
    }
}

void sbits_variable_data_page_numbers_are_correct() {
    insertRecords(1429, 1444, 64);
    /* Number of records * average data size % page size */
    uint32_t numberOfPagesExpected = 69;
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(numberOfPagesExpected - 1, state->nextVarPageId, "SBITS next variable data logical page number is incorrect.");
    uint32_t pageNumber;
    printf("Number of pages expected: %i\n", numberOfPagesExpected);
    void *buffer = (int8_t *)state->buffer + state->pageSize * SBITS_VAR_READ_BUFFER(state->parameters);
    for (uint32_t i = 0; i < numberOfPagesExpected - 1; i++) {
        readVariablePage(state, i);
        memcpy(&pageNumber, buffer, sizeof(id_t));
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(i, pageNumber, "SBITS variable data did not have the correct page number.");
    }
}

void sbits_variable_data_reloads_with_no_data_correctly() {
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_INT8_MESSAGE(8, state->variableDataHeaderSize, "SBITS variableDataHeaderSize did not have the correct value after initializing variable data from a file with no records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8, state->currentVarLoc, "SBITS currentVarLoc did not have the correct value after initializing variable data from a file with no records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, state->minVarRecordId, "SBITS minVarRecordId did not have the correct value after initializing variable data from a file with no records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(75, state->numAvailVarPages, "SBITS numAvailVarPages did not have the correct value after initializing variable data from a file with no records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->nextVarPageId, "SBITS nextVarPageId did not have the correct value after initializing variable data from a file with no records.");
}

void sbits_variable_data_reloads_with_one_page_of_data_correctly() {
    insertRecords(30, 100, 10);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(520, state->currentVarLoc, "SBITS currentVarLoc did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, state->minVarRecordId, "SBITS minVarRecordId did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(74, state->numAvailVarPages, "SBITS numAvailVarPages did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextVarPageId, "SBITS nextVarPageId did not have the correct value after initializing variable data from a file with one page of records.");
}

void sbits_variable_data_reloads_with_sixteen_pages_of_data_correctly() {
    insertRecords(337, 1648, 10);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(8200, state->currentVarLoc, "SBITS currentVarLoc did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, state->minVarRecordId, "SBITS minVarRecordId did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(59, state->numAvailVarPages, "SBITS numAvailVarPages did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(16, state->nextVarPageId, "SBITS nextVarPageId did not have the correct value after initializing variable data from a file with one page of records.");
}

void sbits_variable_data_reloads_with_one_hundred_six_pages_of_data_correctly() {
    insertRecords(2227, 100, 10);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15880, state->currentVarLoc, "SBITS currentVarLoc did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(773, state->minVarRecordId, "SBITS minVarRecordId did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailVarPages, "SBITS numAvailVarPages did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(106, state->nextVarPageId, "SBITS nextVarPageId did not have the correct value after initializing variable data from a file with one page of records.");
}

void sbits_variable_data_reloads_and_queries_with_thirty_one_pages_of_data_correctly() {
    int32_t key = 1000;
    int32_t data = 10;
    insertRecords(651, key, data);
    sbitsFlush(state);
    tearDown();
    initalizeSbitsFromFile();
    int8_t *recordBuffer = (int8_t *)malloc(state->dataSize);
    char keyMessage[80];
    char dataMessage[100];
    char varDataMessage[80];
    char nullReturnMessage[60];
    char variableData[13] = "Hello World!";
    char variableDataBuffer[13];
    sbitsVarDataStream *stream = NULL;
    key = 1001;
    data = 11;
    /* Records inserted before reload */
    for (int i = 0; i < 650; i++) {
        int8_t getResult = sbitsGetVar(state, &key, recordBuffer, &stream);
        snprintf(keyMessage, 80, "SBITS get encountered an error fetching the data for key %i.", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, keyMessage);
        uint32_t streamBytesRead = 0;
        snprintf(nullReturnMessage, 60, "SBITS get var returned null stream for key %i.", key);
        TEST_ASSERT_NOT_NULL_MESSAGE(stream, nullReturnMessage);
        streamBytesRead = sbitsVarDataStreamRead(state, stream, variableDataBuffer, 13);
        snprintf(dataMessage, 100, "SBITS get did not return correct data for a record inserted before reloading (key %i).", key);
        snprintf(varDataMessage, 80, "SBITS get var did not return the correct variable data for key %i.", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, keyMessage);
        TEST_ASSERT_EQUAL_INT32_MESSAGE(data, *((int32_t *)recordBuffer), dataMessage);
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(13, streamBytesRead, "SBITS var data stream did not read the correct number of bytes.");
        TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(variableData, variableDataBuffer, 13, varDataMessage);
        key++;
        data++;
    }
}

void sbits_variable_data_reloads_and_queries_with_two_hundred_forty_seven_pages_of_data_correctly() {
    int32_t key = 6798;
    int32_t data = 13467895;
    insertRecords(5187, key, data);
    sbitsFlush(state);
    tearDown();
    initalizeSbitsFromFile();
    int8_t *recordBuffer = (int8_t *)malloc(state->dataSize);
    char keyMessage[120];
    char dataMessage[100];
    char varDataMessage[80];
    char nullReturnMessage[100];
    char variableData[13] = "Hello World!";
    char variableDataBuffer[13];
    sbitsVarDataStream *stream = NULL;
    key = 9277;
    data = 13470374;
    /* Records inserted before reload */
    for (int i = 0; i < 2708; i++) {
        int8_t getResult = sbitsGetVar(state, &key, recordBuffer, &stream);
        if (i > 1163) {
            snprintf(keyMessage, 120, "SBITS get encountered an error fetching the data for key %i.", key);
            TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, keyMessage);
            snprintf(dataMessage, 100, "SBITS get did not return correct data for a record inserted before reloading (key %i).", key);
            TEST_ASSERT_EQUAL_INT32_MESSAGE(data, *((int32_t *)recordBuffer), dataMessage);
            snprintf(varDataMessage, 80, "SBITS get var did not return the correct variable data for key %i.", key);
            snprintf(nullReturnMessage, 100, "SBITS get var returned null stream for key %i.", key);
            TEST_ASSERT_NOT_NULL_MESSAGE(stream, nullReturnMessage);
            uint32_t streamBytesRead = sbitsVarDataStreamRead(state, stream, variableDataBuffer, 13);
            TEST_ASSERT_EQUAL_UINT32_MESSAGE(13, streamBytesRead, "SBITS var data stream did not read the correct number of bytes.");
            TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(variableData, variableDataBuffer, 13, varDataMessage);
            free(stream);
        } else {
            snprintf(keyMessage, 120, "SBITS get encountered an error fetching the data for key %i. The var data was not detected as being overwritten.", key);
            TEST_ASSERT_EQUAL_INT8_MESSAGE(1, getResult, keyMessage);
            snprintf(dataMessage, 100, "SBITS get did not return correct data for a record inserted before reloading (key %i).", key);
            TEST_ASSERT_EQUAL_INT32_MESSAGE(data, *((int32_t *)recordBuffer), dataMessage);
            snprintf(nullReturnMessage, 100, "SBITS get var did not return null stream for key %i when it should have no variable data.", key);
            TEST_ASSERT_NULL_MESSAGE(stream, nullReturnMessage);
        }
        key++;
        data++;
    }
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(sbits_variable_data_page_numbers_are_correct);
    RUN_TEST(sbits_variable_data_reloads_with_no_data_correctly);
    RUN_TEST(sbits_variable_data_reloads_with_one_page_of_data_correctly);
    RUN_TEST(sbits_variable_data_reloads_with_sixteen_pages_of_data_correctly);
    RUN_TEST(sbits_variable_data_reloads_with_one_hundred_six_pages_of_data_correctly);
    RUN_TEST(sbits_variable_data_reloads_and_queries_with_thirty_one_pages_of_data_correctly);
    RUN_TEST(sbits_variable_data_reloads_and_queries_with_two_hundred_forty_seven_pages_of_data_correctly);
    return UNITY_END();
}
