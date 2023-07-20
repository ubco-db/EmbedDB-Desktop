#include <math.h>
#include <string.h>

#include "../src/sbits/sbits.h"
#include "../src/sbits/utilityFunctions.h"
#include "unity.h"

sbitsState **states;
int numStates = 3;

void setupSbitsInstance(sbitsState **state, int number) {
    state = *((sbitsState *)malloc(sizeof(sbitsState)));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 2;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->numDataPages = 1000;
    state->parameters = SBITS_RESET_DATA;
    state->eraseSizeInPages = 4;
    state->fileInterface = getFileInterface();
    char dataPath[40];
    snprintf(dataPath, 40, "build/artifacts/dataFile%i.bin", number);
    state->dataFile = setupFile(dataPath);
    state->bitmapSize = 0;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int result = sbitsInit(state, 1);
}

void setUp() {
    states = malloc(numStates * sizeof(sbitsState *));
    for (int i = 0; i < numStates; i++)
        setupSbitsInstance(states + i, i);
}

void tearDown(void) {
    for (int i = 0; i < numStates; i++) {
        sbitsState *state = *(states + i);
        sbitsClose(state);
        tearDownFile(state->dataFile);
        free(state->buffer);
        free(state->fileInterface);
    }
    free(state);
}

void insertRecords(int32_t numberOfRecords, int32_t startingKey, int32_t startingData) {
    int32_t key = startingKey;
    int32_t data = startingData;
    int8_t *recordBuffer = (int8_t *)calloc(1, state->recordSize);
    *((int32_t *)recordBuffer) = key;
    *((int32_t *)(recordBuffer + state->keySize)) = startingData;
    for (int32_t i = 0; i < numberOfRecords; i++) {
        *((int32_t *)recordBuffer) += 1;
        *((int32_t *)(recordBuffer + state->keySize)) += 1;
        int8_t insertResult = sbitsPut(state, recordBuffer, (int8_t *)recordBuffer + state->keySize);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "SBITS failed to insert data.");
    }
}

void test_insert_on_multiple_sbits_states() {
    int32_t key = 100;
    int32_t data = 1000;
    int32_t numRecords = 100000;
    char message[120];
    // Insert records
    for (int i = 0; i < numStates; i++) {
        sbitsState *state = *(states + i);
        for (int32_t i = 0; i < numberOfRecords; i++) {
            int8_t insertResult = sbitsPut(state, &key, &data);
            snprintf(message, 120, "sbitsPut returned a non-zero value when inserting key %i into state %i", key, i);
            TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, message);
            key++;
            data++;
        }
    }
    key = 100;
    data = 1000;
    int32_t dataBuffer;
    for (int i = 0; i < numStates; i++) {
        sbitsState *state = *(states + i);
        for (int32_t i = 0; i < numberOfRecords; i++) {
            int8_t getResult = sbitsGet(state, &key, &dataBuffer);
            snprintf(message, 120, "sbitsGet returned a non-zero value when getting key %i from state %i", key, i);
            TEST_ASSERT_EQUAL_INT8_MESSAGE(0, getResult, message);
            snprintf(message, 120, "sbitsGet did not return the correct data for key %i from state %i", key, i)
            TEST_ASSERT_EQUAL_INT32_MESSAGE(data, dataBuffer, message);
            key++;
            data++;
        }
    }
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_insert_on_multiple_sbits_states);
    return UNITY_END();
}
