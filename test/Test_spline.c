#include "../src/sbits/sbits.h"
#include "../src/sbits/utilityFunctions.h"
#include "../src/spline/spline.h"
#include "unity.h"

sbitsState *state;

void setUp(void) {
    state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 8;
    state->dataSize = 8;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 2;
    state->numSplinePoints = 10;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->numDataPages = 500;
    state->parameters = SBITS_RESET_DATA;
    state->eraseSizeInPages = 4;
    state->fileInterface = getFileInterface();
    char dataPath[] = "build/artifacts/dataFile.bin";
    state->dataFile = setupFile(dataPath);
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = sbitsInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "Sbits state failed to init.");
}

void tearDown(void) {
    sbitsClose(state);
    tearDownFile(state->dataFile);
    free(state->buffer);
    free(state->fileInterface);
    free(state);
}

void insertData(size_t numRecords, uint64_t startingKey, int64_t startingData, uint64_t keyIncrement, int64_t dataIncrement);

void spline_adds_points_with_consistent_slope_keys() {
    uint64_t key = 1;
    int64_t data = 2000;
    insertData(10000, 1, 2000, 1, 1);
    sbitsFlush(state);
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(2, state->spl->count, "The spline did not have two points with linear data.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, state->spl->pointsStartIndex, "The spline starting point index was incorrect.");
}

void spline_adds_points_varied_slope_keys() {
    insertData(1000, 100, 200, 1, 1);
    insertData(1000, 1100, 1200, 10, 1);
    insertData(1000, 11100, 2200, 5, 1);
    insertData(1000, 16100, 3200, 1, 1);
    insertData(1000, 17100, 4200, 20, 1);
    sbitsFlush(state);
    printStats(state);
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(6, state->spl->count, "The spline did not have two points with linear data.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, state->spl->pointsStartIndex, "The spline starting point index was incorrect.");
}

void spline_cleans_correctly_with_consistent_slope_keys() {
    insertData(20000, 1, 2000, 1, 1);
    sbitsFlush(state);
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(2, state->spl->count, "The spline did not have two points with linear data.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, state->spl->pointsStartIndex, "The spline starting point index was incorrect.");
    uint64_t expectedKey = 1;
    uint32_t expectedPage = 0;
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedKey, splinePointLocation(state->spl, 0), 8, "The key of the first spline point is incorrect.");
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedPage, (int8_t *)splinePointLocation(state->spl, 0) + 8, 4, "The page of the first spline point is incorrect.");
}

void spline_cleans_correctly_with_changing_slope_keys() {
    uint64_t key = 3456;
    int64_t data = 100;
    for (size_t i = 0; i < 2865; i++) {
        sbitsPut(state, &key, &data);
        key += 15;
        data += 1252;
    }
    for (size_t i = 0; i < 2600; i++) {
        sbitsPut(state, &key, &data);
        key += 13;
        data += 1252;
    }
    for (size_t i = 0; i < 2500; i++) {
        sbitsPut(state, &key, &data);
        key += 5;
        data += 1252;
    }
    for (size_t i = 0; i < 2550; i++) {
        sbitsPut(state, &key, &data);
        key += 45;
        data += 1252;
    }
    for (size_t i = 0; i < 2721; i++) {
        sbitsPut(state, &key, &data);
        key += 1;
        data += 1252;
    }
    for (size_t i = 0; i < 2600; i++) {
        sbitsPut(state, &key, &data);
        key += 2;
        data += 1252;
    }
    for (size_t i = 0; i < 2600; i++) {
        sbitsPut(state, &key, &data);
        key += 11;
        data += 1252;
    }
    sbitsFlush(state);
    printStats(state);
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(7, state->spl->count, "The spline did not have two points with linear data.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, state->spl->pointsStartIndex, "The spline starting point index was incorrect.");
    uint64_t expectedKey = 46431;
    uint32_t expectedPage = 93;
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedKey, splinePointLocation(state->spl, 0), 8, "The key of the first spline point is incorrect.");
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(&expectedPage, (int8_t *)splinePointLocation(state->spl, 0) + 8, 4, "The page of the first spline point is incorrect.");
}

void insertData(size_t numRecords, uint64_t startingKey, int64_t startingData, uint64_t keyIncrement, int64_t dataIncrement) {
    for (size_t i = 0; i < numRecords; i++) {
        sbitsPut(state, &startingKey, &startingData);
        startingKey += keyIncrement;
        startingData += dataIncrement;
    }
    printf("Starting key: %i\n", startingKey);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(spline_adds_points_with_consistent_slope_keys);
    RUN_TEST(spline_adds_points_varied_slope_keys);
    RUN_TEST(spline_cleans_correctly_with_consistent_slope_keys);
    RUN_TEST(spline_cleans_correctly_with_changing_slope_keys);
    return UNITY_END();
}
