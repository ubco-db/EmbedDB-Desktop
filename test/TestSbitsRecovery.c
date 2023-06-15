#include <math.h>
#include <string.h>

#include "../src/sbits/sbits.h"
#include "../src/sbits/utilityFunctions.h"
#include "unity.h"

sbitsState *state;

void setUp(void) {
    state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->startAddress = 0;
    state->endAddress = 100 * state->pageSize;
    state->eraseSizeInPages = 4;
    state->bitmapSize = 0;
    state->parameters;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = sbitsInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "SBITS did not initialize correctly.");
}

void sbits_initalizes_and_inserts_with_recovery() {
    int8_t *data = (int8_t *)malloc(state->recordSize);
    int32_t *sbitsPutResultKey = malloc(sizeof(int32_t));
    int32_t *sbitsPutResultData = malloc(sizeof(int32_t));
    *((int32_t *)data) = 10;
    *((int32_t *)(data + 4)) = 20230615;
    for (int i = 0; i < 1261; i++) {
        *((int32_t *)data) += i;
        *((int32_t *)(data + 4)) += i;
        int8_t result = sbitsPut(state, data, (void *)(data + 4));
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "sbitsPut did not correctly insert data (returned non-zero code)");
    }
}

void tearDown(void) {
    sbitsClose(state);
    free(state);
}

int main(void) {
    UNITY_BEGIN();
    return UNITY_END();
}
