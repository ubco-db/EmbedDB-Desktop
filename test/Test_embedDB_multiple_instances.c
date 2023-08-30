#include <math.h>
#include <string.h>

#include "../src/embedDB/embedDB.h"
#include "../src/embedDB/utilityFunctions.h"
#include "unity.h"

embedDBState **states;

void setupembedDBInstanceKeySize4DataSize4(embedDBState **stateArray, int number) {
    embedDBState *state = (embedDBState *)malloc(sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 2;
    state->numSplinePoints = 300;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->numDataPages = 2000;
    state->parameters = EMBEDDB_RESET_DATA;
    state->eraseSizeInPages = 4;
    state->fileInterface = getFileInterface();
    char dataPath[40];
    snprintf(dataPath, 40, "build/artifacts/dataFile%i.bin", number);
    state->dataFile = setupFile(dataPath);
    state->bitmapSize = 0;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "embedDB init did not return zero when initializing state.");
    *(stateArray + number) = state;
}

void setUp() {}

void tearDown() {}

void insertRecords(embedDBState *state, int32_t numberOfRecords, int32_t startingKey, int32_t startingData) {
    int32_t key = startingKey;
    int32_t data = startingData;
    for (int32_t i = 0; i < numberOfRecords; i++) {
        int8_t insertResult = embedDBPut(state, &key, &data);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "embedDB failed to insert data.");
        key++;
        data++;
    }
    embedDBFlush(state);
}

void queryRecords(embedDBState *state, int32_t numberOfRecords, int32_t startingKey, int32_t startingData) {
    int32_t dataBuffer;
    int32_t key = startingKey;
    int32_t data = startingData;
    char message[120];
    for (int32_t i = 0; i < numberOfRecords; i++) {
        int8_t getResult = embedDBGet(state, &key, &dataBuffer);
        snprintf(message, 120, "embedDBGet returned a non-zero value when getting key %i from state %i", key, i);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
        snprintf(message, 120, "embedDBGet did not return the correct data for key %i from state %i", key, i);
        TEST_ASSERT_EQUAL_INT32_MESSAGE(data, dataBuffer, message);
        key++;
        data++;
    }
}

void insertRecordsFromFile(embedDBState *state, char *fileName, int32_t numRecords) {
    FILE *infile;
    infile = fopen(fileName, "r+b");
    char infileBuffer[512];
    int8_t headerSize = 16;
    int32_t numInserted = 0;
    char message[100];
    while (numInserted < numRecords) {
        if (0 == fread(infileBuffer, state->pageSize, 1, infile))
            break;
        int16_t count = *((int16_t *)(infileBuffer + 4));
        for (int16_t i = 0; i < count; i++) {
            void *buf = (infileBuffer + headerSize + i * state->recordSize);
            int8_t putResult = embedDBPut(state, buf, (void *)((int8_t *)buf + 4));
            snprintf(message, 100, "embedDBPut returned non-zero value for insert of key %i", *((uint32_t *)buf));
            TEST_ASSERT_EQUAL_INT8_MESSAGE(0, putResult, message);
            numInserted++;
            if (numInserted >= numRecords) {
                break;
            }
        }
    }
    embedDBFlush(state);
    fclose(infile);
}

void insertRecordsFromFileWithVarData(embedDBState *state, char *fileName, int32_t numRecords) {
    FILE *infile;
    infile = fopen(fileName, "r+b");
    char infileBuffer[512];
    int8_t headerSize = 16;
    int32_t numInserted = 0;
    char message[100];
    char *varData = calloc(30, sizeof(char));
    while (numInserted < numRecords) {
        if (0 == fread(infileBuffer, state->pageSize, 1, infile))
            break;
        int16_t count = *((int16_t *)(infileBuffer + 4));
        for (int16_t i = 0; i < count; i++) {
            void *buf = (infileBuffer + headerSize + i * (state->keySize + state->dataSize));
            snprintf(varData, 30, "Hello world %i", *((uint32_t *)buf));
            int8_t putResult = embedDBPutVar(state, buf, (void *)((int8_t *)buf + 4), varData, strlen(varData));
            snprintf(message, 100, "embedDBPut returned non-zero value for insert of key %i", *((uint32_t *)buf));
            TEST_ASSERT_EQUAL_INT8_MESSAGE(0, putResult, message);
            numInserted++;
            if (numInserted >= numRecords) {
                break;
            }
        }
    }
    free(varData);
    embedDBFlush(state);
    fclose(infile);
}

void queryRecordsFromFile(embedDBState *state, char *fileName, int32_t numRecords) {
    FILE *infile;
    infile = fopen(fileName, "r+b");
    char infileBuffer[512];
    int8_t headerSize = 16;
    int32_t numRead = 0;
    int8_t *dataBuffer = malloc(state->dataSize * sizeof(int8_t));
    char message[100];
    while (numRead < numRecords) {
        if (0 == fread(infileBuffer, state->pageSize, 1, infile))
            break;
        int16_t count = *((int16_t *)(infileBuffer + 4));
        for (int16_t i = 0; i < count; i++) {
            void *buf = (infileBuffer + headerSize + i * state->recordSize);
            int8_t getResult = embedDBGet(state, buf, dataBuffer);
            snprintf(message, 100, "embedDBGet was not able to find the data for key %i", *((uint32_t *)buf));
            TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
            snprintf(message, 100, "embedDBGet did not return the correct data for key %i", *((uint32_t *)buf));
            TEST_ASSERT_EQUAL_MEMORY_MESSAGE(buf + 4, dataBuffer, state->dataSize, message);
            numRead++;
            if (numRead >= numRecords)
                break;
        }
    }
    TEST_ASSERT_EQUAL_INT32_MESSAGE(numRecords, numRead, "The number of records read was not equal to the number of records inserted.");
    fclose(infile);
}

void queryRecordsFromFileWithVarData(embedDBState *state, char *fileName, int32_t numRecords) {
    FILE *infile;
    infile = fopen(fileName, "r+b");
    char infileBuffer[512];
    int8_t headerSize = 16;
    int32_t numRead = 0;
    int8_t *dataBuffer = malloc(state->dataSize * sizeof(int8_t));
    char *varDataBuffer = calloc(30, sizeof(char));
    char *varDataExpected = calloc(30, sizeof(char));
    char message[100];
    while (numRead < numRecords) {
        if (0 == fread(infileBuffer, state->pageSize, 1, infile))
            break;
        int16_t count = *((int16_t *)(infileBuffer + 4));
        for (int16_t i = 0; i < count; i++) {
            void *buf = (infileBuffer + headerSize + i * (state->keySize + state->dataSize));
            snprintf(varDataExpected, 30, "Hello world %i", *((uint32_t *)buf));
            embedDBVarDataStream *stream = NULL;
            int8_t getResult = embedDBGetVar(state, buf, dataBuffer, &stream);
            snprintf(message, 100, "embedDBGetVar was not able to find the data for key %i", *((uint32_t *)buf));
            TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
            snprintf(message, 100, "embedDBGetBar did not return the correct data for key %i", *((uint32_t *)buf));
            TEST_ASSERT_EQUAL_MEMORY_MESSAGE(buf + 4, dataBuffer, state->dataSize, message);
            uint32_t streamBytesRead = embedDBVarDataStreamRead(state, stream, varDataBuffer, strlen(varDataExpected));
            snprintf(message, 100, "embedDBGetVar did not return the correct variable data for key %i", *((uint32_t *)buf));

            TEST_ASSERT_EQUAL_MEMORY_MESSAGE(varDataExpected, varDataBuffer, strlen(varDataExpected), message);
            numRead++;
            free(stream);
            if (numRead >= numRecords)
                break;
        }
    }
    TEST_ASSERT_EQUAL_INT32_MESSAGE(numRecords, numRead, "The number of records read was not equal to the number of records inserted.");
    fclose(infile);
    free(dataBuffer);
    free(varDataBuffer);
    free(varDataExpected);
}

void setupembedDBInstanceKeySize4DataSize12(embedDBState **stateArray, int number) {
    embedDBState *state = (embedDBState *)malloc(sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = 12;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 4;
    state->numSplinePoints = 300;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->numDataPages = 20000;
    state->numIndexPages = 1000;
    state->parameters = EMBEDDB_RESET_DATA | EMBEDDB_USE_INDEX;
    state->eraseSizeInPages = 4;
    state->fileInterface = getFileInterface();
    char path[40];
    snprintf(path, 40, "build/artifacts/dataFile%i.bin", number);
    state->dataFile = setupFile(path);
    snprintf(path, 40, "build/artifacts/indexFile%i.bin", number);
    state->indexFile = setupFile(path);
    state->bitmapSize = 1;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "embedDB init did not return zero when initializing state.");
    *(stateArray + number) = state;
}

void setupembedDBInstanceKeySize4DataSize12WithVarData(embedDBState **stateArray, int number) {
    embedDBState *state = (embedDBState *)malloc(sizeof(embedDBState));
    state->keySize = 4;
    state->dataSize = 12;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->numSplinePoints = 300;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->numDataPages = 22000;
    state->numIndexPages = 1000;
    state->numVarPages = 44000;
    state->parameters = EMBEDDB_RESET_DATA | EMBEDDB_USE_INDEX | EMBEDDB_USE_VDATA;
    state->eraseSizeInPages = 4;
    state->fileInterface = getFileInterface();
    char path[40];
    snprintf(path, 40, "build/artifacts/dataFile%i.bin", number);
    state->dataFile = setupFile(path);
    snprintf(path, 40, "build/artifacts/indexFile%i.bin", number);
    state->indexFile = setupFile(path);
    snprintf(path, 40, "build/artifacts/varFile%i.bin", number);
    state->varFile = setupFile(path);
    state->bitmapSize = 1;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = embedDBInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "embedDB init did not return zero when initializing state.");
    *(stateArray + number) = state;
}

void test_insert_on_multiple_embedDB_states() {
    int numStates = 3;
    states = malloc(numStates * sizeof(embedDBState *));
    for (int i = 0; i < numStates; i++)
        setupembedDBInstanceKeySize4DataSize4(states, i);
    int32_t key = 100;
    int32_t data = 1000;
    int32_t numRecords = 100000;

    // Insert records
    for (int i = 0; i < numStates; i++) {
        embedDBState *state = *(states + i);
        insertRecords(state, numRecords, key, data);
    }

    for (int i = 0; i < numStates; i++) {
        embedDBState *state = *(states + i);
        queryRecords(state, numRecords, key, data);
    }

    for (int i = 0; i < numStates; i++) {
        embedDBState *state = *(states + i);
        embedDBClose(state);
        tearDownFile(state->dataFile);
        free(state->buffer);
        free(state->fileInterface);
    }
    free(states);
}

void test_insert_from_files_with_index_multiple_states() {
    int numStates = 3;
    states = malloc(numStates * sizeof(embedDBState *));
    for (int i = 0; i < numStates; i++)
        setupembedDBInstanceKeySize4DataSize12(states, i);

    insertRecordsFromFile(*(states), "data/uwa500K.bin", 500000);
    insertRecordsFromFile(*(states + 1), "data/ethylene_CO.bin", 400000);
    queryRecordsFromFile(*(states), "data/uwa500K.bin", 500000);
    insertRecordsFromFile(*(states + 2), "data/PRSA_Data_Hongxin.bin", 33311);
    queryRecordsFromFile(*(states + 1), "data/ethylene_CO.bin", 400000);
    queryRecordsFromFile(*(states + 2), "data/PRSA_Data_Hongxin.bin", 33311);

    for (int i = 0; i < numStates; i++) {
        embedDBState *state = *(states + i);
        embedDBClose(state);
        tearDownFile(state->dataFile);
        tearDownFile(state->indexFile);
        free(state->buffer);
        free(state->fileInterface);
    }
    free(states);
}

void test_insert_from_files_with_vardata_multiple_states() {
    int numStates = 4;
    states = malloc(numStates * sizeof(embedDBState *));
    for (int i = 0; i < numStates; i++)
        setupembedDBInstanceKeySize4DataSize12WithVarData(states, i);

    insertRecordsFromFileWithVarData(*(states), "data/uwa500K.bin", 500000);
    insertRecordsFromFileWithVarData(*(states + 1), "data/measure1_smartphone_sens.bin", 18354);
    queryRecordsFromFileWithVarData(*(states), "data/uwa500K.bin", 500000);
    insertRecordsFromFileWithVarData(*(states + 2), "data/ethylene_CO.bin", 185589);
    insertRecordsFromFileWithVarData(*(states + 3), "data/position.bin", 1518);
    queryRecordsFromFileWithVarData(*(states + 2), "data/ethylene_CO.bin", 185589);
    queryRecordsFromFileWithVarData(*(states + 3), "data/position.bin", 1518);
    queryRecordsFromFileWithVarData(*(states + 1), "data/measure1_smartphone_sens.bin", 18354);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_insert_on_multiple_embedDB_states);
    RUN_TEST(test_insert_from_files_with_index_multiple_states);
    RUN_TEST(test_insert_from_files_with_vardata_multiple_states);
    return UNITY_END();
}
