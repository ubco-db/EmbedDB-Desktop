#include "sbits/sbits.h"
#include "sbits/utilityFunctions.h"
#include "spline/spline.h"

int initializeSbits(sbitsState *state);
int insertRecords(sbitsState *state, uint32_t numberOfRecords);

int main() {
    sbitsState *state = (sbitsState *)malloc(sizeof(sbitsState));
    int initStatus = initializeSbits(state);
    if (initStatus != 0) {
        printf("Init not successfull.\n");
        return initStatus;
    }
    int insertStatus = insertRecords(state, 3781);
    if (insertStatus != 0) {
        return insertStatus;
    }

    // printf("Spline Stats After Initial Inserts: \n");
    // splinePrint(state->spl);
    sbitsClose(state);

    /* Initalize state again */
    state = (sbitsState *)malloc(sizeof(sbitsState));
    initStatus = initializeSbits(state);
    if (initStatus != 0) {
        printf("Re-init was not successful.\n");
        return initStatus;
    }
    // splinePrint(state->spl);
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
    state->parameters = 0;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    return sbitsInit(state, 1);
}

int insertRecords(sbitsState *state, uint32_t numberOfRecords) {
    printf("Record size is %i\n", state->recordSize);
    int8_t *data = (int8_t *)malloc(state->recordSize);
    if (data == NULL) {
        printf("There was an error\n");
        return -1;
    }
    *((int32_t *)data) = 10;
    *((int32_t *)(data + 4)) = 20230615;
    for (int i = 0; i < numberOfRecords; i++) {
        *((int32_t *)data) += i;
        *((int32_t *)(data + 4)) += i;
        int8_t result = sbitsPut(state, data, (void *)(data + 4));
        if (result != 0) {
            return result;
        }
    }
    free(data);
    return 0;
}
