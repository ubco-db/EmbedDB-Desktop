#include <math.h>
#include <string.h>

#include "../src/sbits/sbits.h"
#include "../src/sbits/utilityFunctions.h"
#include "unity.h"

sbitsState *state;

typedef struct Node {
    int32_t key;
    void *data;
    uint32_t length;
    struct Node *next;
} Node;

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

uint32_t randomData(void **data, uint32_t sizeLowerBound, uint32_t sizeUpperBound) {
    uint32_t size = rand() % (sizeUpperBound - sizeLowerBound) + sizeLowerBound;
    *data = malloc(size);
    for (uint32_t i = 0; i < size; i++) {
        *((uint8_t *)(*data) + i) = rand() % UINT8_MAX;
    }
    return size;
}

void randomVarData(uint32_t chance, uint32_t sizeLowerBound, uint32_t sizeUpperBound, uint8_t *usingVarData, uint32_t *length, void **varData) {
    *usingVarData = (rand() % chance) == 0;
    if (usingVarData) {
        *length = randomData(varData, sizeLowerBound, sizeUpperBound);
    } else {
        *length = 0;
        *varData = NULL;
    }
}

void insertRecordsWithRandomData(int32_t numberOfRecords, int32_t startingKey, int32_t startingData, uint32_t chaneOfVariableData, uint32_t lowerVariableDataSizeBound, uint32_t upperVariableDataSizeBound, Node **linkedList) {
    int32_t key = startingKey;
    int32_t data = startingData;
    int8_t *recordBuffer = (int8_t *)calloc(1, state->recordSize);
    *linkedList = malloc(sizeof(Node));
    Node *linkedListTail = *linkedList;
    for (int32_t i = 0; i < numberOfRecords; i++) {
        void *variableData = NULL;
        uint8_t hasVarData = 0;
        uint32_t length;
        randomVarData(chaneOfVariableData, lowerVariableDataSizeBound, upperVariableDataSizeBound, &hasVarData, &length, &variableData);
        *((int32_t *)recordBuffer) += 1;
        *((int32_t *)(recordBuffer + state->keySize)) += 1;
        int8_t insertResult = sbitsPutVar(state, recordBuffer, (void *)(recordBuffer + state->keySize), hasVarData ? variableData : NULL, length);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "SBITS failed to insert data.");
        linkedListTail->key = i;
        linkedListTail->data = variableData;
        linkedListTail->length = length;
        linkedListTail->next = malloc(sizeof(Node));
        linkedListTail = linkedListTail->next;
        linkedListTail->length = 0;
    }
}

void insertRecords(int32_t numberOfRecords, int32_t startingKey, int32_t startingData, Node **linkedList) {
    int32_t key = startingKey;
    int32_t data = startingData;
    int8_t *recordBuffer = (int8_t *)calloc(1, state->recordSize);
    *linkedList = malloc(sizeof(Node));
    Node *linkedListTail = *linkedList;
    char variableData[13] = "Hello World!";
    for (int32_t i = 0; i < numberOfRecords; i++) {
        *((int32_t *)recordBuffer) += 1;
        *((int32_t *)(recordBuffer + state->keySize)) += 1;
        int8_t insertResult = sbitsPutVar(state, recordBuffer, (void *)(recordBuffer + state->keySize), variableData, 13);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "SBITS failed to insert data.");
        linkedListTail->key = i;
        linkedListTail->data = variableData;
        linkedListTail->length = 13;
        linkedListTail->next = malloc(sizeof(Node));
        linkedListTail = linkedListTail->next;
        linkedListTail->length = 0;
    }
}

void sbits_variable_data_page_numbers_are_correct() {
    Node *linkedList;
    insertRecordsWithRandomData(1429, 1444, 64, 1, 10, 20, &linkedList);
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
    free(linkedList);
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
    Node *linkedList;
    insertRecordsWithRandomData(15, 100, 10, 1, 25, 41, &linkedList);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(520, state->currentVarLoc, "SBITS currentVarLoc did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, state->minVarRecordId, "SBITS minVarRecordId did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(74, state->numAvailVarPages, "SBITS numAvailVarPages did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(1, state->nextVarPageId, "SBITS nextVarPageId did not have the correct value after initializing variable data from a file with one page of records.");
    free(linkedList);
}

void sbits_variable_data_reloads_with_fifteen_pages_of_data_correctly() {
    Node *linkedList;
    insertRecordsWithRandomData(525, 100, 10, 10, 80, 120, &linkedList);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(7688, state->currentVarLoc, "SBITS currentVarLoc did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, state->minVarRecordId, "SBITS minVarRecordId did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(60, state->numAvailVarPages, "SBITS numAvailVarPages did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, state->nextVarPageId, "SBITS nextVarPageId did not have the correct value after initializing variable data from a file with one page of records.");
    free(linkedList);
}

void sbits_variable_data_reloads_with_one_hundred_six_pages_of_data_correctly() {
    Node *linkedList;
    insertRecords(2227, 100, 10, &linkedList);
    tearDown();
    initalizeSbitsFromFile();
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15880, state->currentVarLoc, "SBITS currentVarLoc did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(672, state->minVarRecordId, "SBITS minVarRecordId did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(0, state->numAvailVarPages, "SBITS numAvailVarPages did not have the correct value after initializing variable data from a file with one page of records.");
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(106, state->nextVarPageId, "SBITS nextVarPageId did not have the correct value after initializing variable data from a file with one page of records.");
    free(linkedList);
}

void sbits_variable_data_reloads_and_queries_with_thirty_one_pages_of_data_correctly() {
    Node *linkedList;
    int32_t key = 1000;
    int64_t data = 10;
    insertRecords(651, key, data, &linkedList);
    tearDown();
    initalizeSbitsFromFile();
    int8_t *recordBuffer = (int8_t *)malloc(state->dataSize);
    char keyMessage[80];
    char dataMessage[100];
    char variableData[13] = "Hello World!";
    char variableDataBuffer[13];
    /* Records inserted before reload */
    for (int i = 0; i < 3888; i++) {
        int8_t getResult = sbitsGetVar(state, &key, recordBuffer);
        snprintf(keyMessage, 80, "SBITS get encountered an error fetching the data for key %i.", key);
        snprintf(dataMessage, 100, "SBITS get did not return correct data for a record inserted before reloading (key %i).", key);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, keyMessage);
        TEST_ASSERT_EQUAL_INT64_MESSAGE(data, *((int64_t *)recordBuffer), dataMessage);
        key++;
        data++;
    }
    free(linkedList);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(sbits_variable_data_page_numbers_are_correct);
    RUN_TEST(sbits_variable_data_reloads_with_no_data_correctly);
    RUN_TEST(sbits_variable_data_reloads_with_one_page_of_data_correctly);
    RUN_TEST(sbits_variable_data_reloads_with_fifteen_pages_of_data_correctly);
    RUN_TEST(sbits_variable_data_reloads_with_one_hundred_six_pages_of_data_correctly);
    return UNITY_END();
}
