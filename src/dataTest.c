#include "sbits/sbits.h"
#include "sbits/utilityFunctions.h"
#include "spline/spline.h"

int initializeSbits(sbitsState *state);
int insertRecords(sbitsState *state, uint32_t numberOfRecords, int32_t startingKey, int32_t startingData);
int queryAllRecords(sbitsState *state, uint32_t numberOfRecords, int32_t startingKey, int32_t startingData);
int queryRecord(sbitsState *state, int32_t key, int32_t data);

int main() {
    sbitsState *state = (sbitsState *)malloc(sizeof(sbitsState));
    int initStatus = initializeSbits(state);
    if (initStatus != 0) {
        printf("Init not successfull.\n");
        return initStatus;
    }
    int insertStatus = insertRecords(state, 3781, 10, 2465);
    if (insertStatus != 0) {
        return insertStatus;
    }

    printf("\nSpline Stats After Initial Inserts: \n");
    splinePrint(state->spl);
    queryRecord(state, 3728, 3105);

    sbitsClose(state);

    // /* Initalize state again */
    // state = (sbitsState *)malloc(sizeof(sbitsState));
    // initStatus = initializeSbits(state);
    // if (initStatus != 0) {
    //     printf("Re-init was not successful.\n");
    //     return initStatus;
    // }
    // printf("\nSpline Stats After Reload: \n");
    // splinePrint(state->spl);

    // printf("\nQuery Testing:\n");
    // queryRecord(state, 286156, 288611);
    // // int queryStatus = queryAllRecords(state, 3150 + 630, 198775, 201230);

    return 0;
}

int initializeSbits(sbitsState *state) {
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->startAddress = 0;
    state->endAddress = 50 * state->pageSize;
    state->eraseSizeInPages = 4;
    state->bitmapSize = 0;
    state->parameters = SBITS_RESET_DATA;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    return sbitsInit(state, 1);
}

int insertRecords(sbitsState *state, uint32_t numberOfRecords, int32_t startingKey, int32_t startingData) {
    printf("Record size is %i\n", state->recordSize);
    int8_t *data = (int8_t *)malloc(state->recordSize);
    if (data == NULL) {
        printf("There was an error\n");
        return -1;
    }
    *((int32_t *)data) = startingKey;
    *((int32_t *)(data + 4)) = startingData;
    for (int32_t i = 0; i < numberOfRecords; i++) {
        *((int32_t *)data) += 1;
        *((int32_t *)(data + 4)) += 1;
        int8_t result = sbitsPut(state, data, (void *)(data + 4));
        if (result != 0) {
            return result;
        }
    }
    free(data);
    return 0;
}

int queryAllRecords(sbitsState *state, uint32_t numberOfRecords, int32_t startingKey, int32_t startingData) {
    int32_t *result = (int32_t *)malloc(state->recordSize);
    int32_t key = startingKey;
    int32_t data = startingData;
    for (uint32_t i = 630; i < numberOfRecords; i++) {
        int8_t result = sbitsGet(state, &key, (void *)result);

        if (result != 0) {
            printf("ERROR: Failed to find: %lu\n", key);
            return 1;
        }
        if (*((int32_t *)result) != data) {
            printf("ERROR: Wrong data for: %lu\n", key);
            printf("Key: %lu Data: %lu\n", key,
                   *((int32_t *)result));
            return 1;
        }

        key += i;
        data += i;
    }
    return 0;
}

int queryRecord(sbitsState *state, int32_t key, int32_t data) {
    int8_t *recordBuffer = (int8_t *)malloc(state->recordSize);
    int8_t result = sbitsGet(state, &key, recordBuffer);

    if (result != 0)
        printf("ERROR: Failed to find: %lu\n", key);
    if (*((int32_t *)recordBuffer) != data) {
        printf("ERROR: Wrong data for: %lu\n", key);
        printf("Key: %lu Data: %lu\n", key,
               *((int32_t *)recordBuffer));
        return 1;
    }

    return 0;
}
