#include "../lib/unity/unity.h"
#include "../src/sbits.h"

sbitsState *state;

void updateBitmapInt16(void *data, void *bm) {
    int32_t val = *((int32_t *)data);
    uint16_t *bmval = (uint16_t *)bm;

    /* Using a demo range of 0 to 100 */
    // int16_t stepSize = 100 / 15;
    int16_t stepSize = 450 / 15;  // Temperature data in F. Scaled by 10. */
    int16_t minBase = 320;
    int32_t current = minBase;
    uint16_t num = 32768;
    while (val > current) {
        current += stepSize;
        num = num / 2;
    }
    if (num == 0)
        num = 1; /* Always set last bit if value bigger than largest cutoff */
    *bmval = *bmval | num;
}

int8_t inBitmapInt16(void *data, void *bm) {
    uint16_t *bmval = (uint16_t *)bm;

    uint16_t tmpbm = 0;
    updateBitmapInt16(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/* A 64-bit bitmap on a 32-bit int value */
void updateBitmapInt64(void *data, void *bm) {
    int32_t val = *((int32_t *)data);

    int16_t stepSize = 10;  // Temperature data in F. Scaled by 10. */
    int32_t current = 320;
    int8_t bmsize = 63;
    int8_t count = 0;

    while (val > current && count < 63) {
        current += stepSize;
        count++;
    }
    uint8_t b = 128;
    int8_t offset = count / 8;
    b = b >> (count & 7);

    *((char *)((char *)bm + offset)) = *((char *)((char *)bm + offset)) | b;
}

int8_t inBitmapInt64(void *data, void *bm) {
    uint64_t *bmval = (uint64_t *)bm;

    uint64_t tmpbm = 0;
    updateBitmapInt64(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

int8_t int32Comparator(void *a, void *b) {
    int32_t result = *((int32_t *)a) - *((int32_t *)b);
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}

void setUp(void) {
    state = (sbitsState *)malloc(sizeof(sbitsState));
    int8_t M = 4;
    int32_t numRecords = 500000;   // default values
    int32_t testRecords = 500000;  // default values

    state->recordSize = 16;
    state->keySize = 4;
    state->dataSize = 12;
    state->pageSize = 512;
    state->bitmapSize = 0;
    state->bufferSizeInBlocks = M;
    state->buffer =
        malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    int8_t *recordBuffer = (int8_t *)malloc(state->recordSize);

    /* Address level parameters */
    state->startAddress = 0;
    state->endAddress =
        state->pageSize * numRecords /
        10; /* Modify this value lower to test wrap around */
    state->eraseSizeInPages = 4;
    // state->parameters = SBITS_USE_MAX_MIN | SBITS_USE_BMAP |
    // SBITS_USE_INDEX;
    state->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX;
    // state->parameters =  0;
    if (SBITS_USING_INDEX(state->parameters) == 1)
        state->endAddress +=
            state->pageSize * (state->eraseSizeInPages * 2);
    if (SBITS_USING_BMAP(state->parameters))
        state->bitmapSize = 8;

    /* Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt16;
    state->updateBitmap = updateBitmapInt16;
    state->inBitmap = inBitmapInt64;
    state->updateBitmap = updateBitmapInt64;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
}

void tearDown(void) {
    free(state);
}

void sbtisInitShouldInitState(void) {
    int result = sbitsInit(state, 1);
    TEST_ASSERT_EQUAL_INT_MESSAGE(result, 0, "There was an error with sbitsInit. Sbits was not initalized correctly.");
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(sbtisInitShouldInitState);
    return UNITY_END();
}
