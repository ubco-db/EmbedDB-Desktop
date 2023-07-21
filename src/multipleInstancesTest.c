#include <errno.h>
#include <string.h>
#include <time.h>

#include "sbits/sbits.h"
#include "sbits/utilityFunctions.h"

sbitsState **states;

int setupSbitsInstance(sbitsState **stateArray, int number) {
    sbitsState *state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 2;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->numDataPages = 10000;
    state->parameters = SBITS_RESET_DATA;
    state->eraseSizeInPages = 4;
    state->fileInterface = getFileInterface();
    char dataPath[40];
    snprintf(dataPath, 40, "build/artifacts/dataFile%i.bin", number);
    state->dataFile = setupFile(dataPath);
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    state->bitmapSize = 0;
    int8_t result = sbitsInit(state, 1);
    if (result != 0) {
        printf("There was an error with the sbits init\n");
        return 1;
    }
    *(stateArray + number) = state;
    return 0;
}

int insertRecords(sbitsState *state, int32_t numberOfRecords, int32_t startingKey, int32_t startingData) {
    int32_t key = startingKey;
    int32_t data = startingData;
    for (int32_t i = 0; i < numberOfRecords; i++) {
        int8_t insertResult = sbitsPut(state, &key, &data);
        if (insertResult != 0) {
            printf("ERROR: Failed to insert for key: %lu\n", key);
            return 1;
        }
        key++;
        data++;
    }
    sbitsFlush(state);
}

int queryRecordsLinearly(sbitsState *state, uint32_t numberOfRecords, int32_t startingKey, int64_t startingData) {
    int32_t *result = (int32_t *)malloc(state->recordSize);
    int32_t key = startingKey;
    int64_t data = startingData;
    char message[100];
    for (int32_t i = 0; i < numberOfRecords; i++) {
        int8_t getStatus = sbitsGet(state, &key, (void *)result);
        if (getStatus != 0) {
            printf("ERROR: Failed to find key: %i\n", key);
            return 1;
        }
        if (*((int32_t *)result) != data) {
            printf("ERROR: Wrong data for: %i\n", key);
            printf("Key: %lu Data: %lu\n", key, *((int64_t *)result));
            free(result);
            return 1;
        }
        key++;
        data++;
    }
    free(result);
    printf("Success :)");
    return 0;
}

int main() {
    int32_t numStates = 5;
    sbitsState **states = malloc(numStates * sizeof(sbitsState *));
    for (int32_t i = 0; i < numStates; i++)
        setupSbitsInstance(states, i);
    insertRecords(*(states + 0), 100000, 100, 1000);
    insertRecords(*(states + 2), 64987, 635, 5684);
    queryRecordsLinearly(*(states + 0), 100000, 100, 1000);
    insertRecords(*(states + 2), 62489, 100645, 6845);
    queryRecordsLinearly(*(states + 2), 64987, 635, 5684);
    queryRecordsLinearly(*(states + 0), 100000, 100, 1000);
    queryRecordsLinearly(*(states + 2), 62489, 100645, 6845);
    return 0;
}
