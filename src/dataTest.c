#include "sbits/sbits.h"
#include "sbits/utilityFunctions.h"

int initializeSbits(sbitsState *state);

int main() {
    sbitsState *state = (sbitsState *)malloc(sizeof(sbitsState));
    int initStatus = initializeSbits(state);
    if (initStatus != 0) {
        return initStatus;
    }
    return 0;
}

int initializeSbits(sbitsState *state) {
    int32_t numRecords = 1000;
    
    state->recordSize = 16;
    state->keySize = 4;
    state->dataSize = 12;
    state->pageSize = 512;
    state->bitmapSize = 0;
    state->bufferSizeInBlocks = 4;
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    int8_t *recordBuffer = (int8_t *)malloc(state->recordSize);

    /* Address level parameters */
    state->startAddress = 0;
    state->endAddress = state->pageSize * numRecords / 10;
    state->eraseSizeInPages = 4;
    state->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX;
    if (SBITS_USING_INDEX(state->parameters) == 1)
        state->endAddress +=
            state->pageSize * (state->eraseSizeInPages * 2);
    if (SBITS_USING_BMAP(state->parameters))
        state->bitmapSize = 1;

    /* Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;

    /* Initialize SBITS structure with parameters */
    return sbitsInit(state, 1);
}
