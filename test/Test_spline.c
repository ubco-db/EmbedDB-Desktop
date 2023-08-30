/******************************************************************************/
/**
 * @file		Test_spline.c
 * @author		EmbedDB Team (See Authors.md)
 * @brief		Test EmbedDB spline implementation.
 * @copyright	Copyright 2023
 * 			    EmbedDB Team
 * @par Redistribution and use in source and binary forms, with or without
 * 	modification, are permitted provided that the following conditions are met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 * 	this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 * 	this list of conditions and the following disclaimer in the documentation
 * 	and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its contributors
 * 	may be used to endorse or promote products derived from this software without
 * 	specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * 	AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * 	IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * 	ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * 	LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * 	CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * 	SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * 	INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * 	CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * 	ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * 	POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/

#include "../src/embedDB/embedDB.h"
#include "../src/embedDB/utilityFunctions.h"
#include "../src/spline/spline.h"
#include "unity.h"

embedDBState *state;
void *splineBuffer;

void setUp(void) {
    state = (embedDBState *)malloc(sizeof(embedDBState));
    state->keySize = 8;
    state->dataSize = 8;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 2;
    state->numSplinePoints = 5;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->numDataPages = 500;
    state->parameters = EMBEDDB_RESET_DATA;
    state->eraseSizeInPages = 4;
    state->fileInterface = getFileInterface();
    char dataPath[] = "build/artifacts/dataFile.bin";
    state->dataFile = setupFile(dataPath);
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = embedDBInit(state, 0);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "embedDB state failed to init.");
    state->cleanSpline = 0;
    splineBuffer = malloc(60);
}

void tearDown(void) {
    embedDBClose(state);
    tearDownFile(state->dataFile);
    free(state->buffer);
    free(state->fileInterface);
    free(state);
    free(splineBuffer);
}

void insertData(size_t numRecords, uint64_t startingKey, int64_t startingData, uint64_t keyIncrement, int64_t dataIncrement);

void spline_adds_points_with_consistent_slope_keys() {
    uint64_t key = 1;
    int64_t data = 2000;
    insertData(10000, 1, 2000, 1, 1);
    embedDBFlush(state);
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(2, state->spl->count, "The spline did not have two points for keys inserted at a constant rate.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, state->spl->pointsStartIndex, "The spline starting point index was incorrect.");
}

void spline_adds_points_varied_slope_keys() {
    insertData(992, 100, 200, 1, 1);
    embedDBFlush(state);
    insertData(992, 1092, 1200, 10, 1);
    embedDBFlush(state);
    insertData(992, 11012, 2200, 5, 1);
    embedDBFlush(state);
    insertData(992, 15972, 2200, 6, 1);
    embedDBFlush(state);
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(5, state->spl->count, "The spline did not have the correct number of points for keys inserted at varying rates.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, state->spl->pointsStartIndex, "The spline starting point index was incorrect.");
}

void spline_cleans_correctly_with_consistent_slope_keys() {
    insertData(20000, 1, 2000, 1, 1);
    embedDBFlush(state);
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(2, state->spl->count, "The spline did not have two points for keys inserted at a constant rate.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(0, state->spl->pointsStartIndex, "The spline starting point index was incorrect.");
    uint64_t expectedKey = 1;
    int32_t expectedPage = 0;
    memcpy(splineBuffer, &expectedKey, state->keySize);
    memcpy((int8_t *)splineBuffer + 8, &expectedPage, sizeof(expectedPage));
    expectedKey = 19996;
    expectedPage = 645;
    memcpy((int8_t *)splineBuffer + 12, &expectedKey, state->keySize);
    memcpy((int8_t *)splineBuffer + 20, &expectedPage, sizeof(expectedPage));
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE(splineBuffer, state->spl->points, 24, "The array of spline points was not generated correctly.");
}

void spline_cleans_correctly_with_changing_slope_keys() {
    insertData(248, 100, 45, 1, 1);
    embedDBFlush(state);
    insertData(341, 348, 234, 3, 1);
    embedDBFlush(state);
    insertData(1395, 1371, 45, 10, 23);
    embedDBFlush(state);
    insertData(62, 15321, 10023, 5, 15);
    embedDBFlush(state);
    insertData(4433, 15631, 1, 12, 1);
    embedDBFlush(state);
    insertData(2697, 68827, 1, 1, 10);
    embedDBFlush(state);
    insertData(744, 71524, 32, 67, 80);
    embedDBFlush(state);
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(5, state->spl->count, "The spline did not have the correct number of points for keys inserted at varying rates.");
    TEST_ASSERT_EQUAL_UINT64_MESSAGE(3, state->spl->pointsStartIndex, "The spline starting point index was incorrect when some of the points were overwritten.");

    /* Build points expected points array in splineBuffer */
    uint64_t expectedKey = 15321;
    int32_t expectedPage = 64;
    memcpy((int8_t *)splineBuffer + 36, &expectedKey, state->keySize);
    memcpy((int8_t *)splineBuffer + 44, &expectedPage, 4);
    expectedKey = 15631;
    expectedPage = 66;
    memcpy((int8_t *)splineBuffer + 48, &expectedKey, state->keySize);
    memcpy((int8_t *)splineBuffer + 56, &expectedPage, sizeof(expectedPage));
    expectedKey = 68827;
    expectedPage = 209;
    memcpy((int8_t *)splineBuffer + 0, &expectedKey, state->keySize);
    memcpy((int8_t *)splineBuffer + 8, &expectedPage, sizeof(expectedPage));
    expectedKey = 71524;
    expectedPage = 296;
    memcpy((int8_t *)splineBuffer + 12, &expectedKey, state->keySize);
    memcpy((int8_t *)splineBuffer + 20, &expectedPage, sizeof(expectedPage));
    expectedKey = 119295;
    expectedPage = 319;
    memcpy((int8_t *)splineBuffer + 24, &expectedKey, state->keySize);
    memcpy((int8_t *)splineBuffer + 32, &expectedPage, sizeof(expectedPage));

    /* Test that the spline array was generted correctly */
    TEST_ASSERT_EQUAL_MEMORY_MESSAGE((int8_t *)splineBuffer, (int8_t *)state->spl->points, 60, "The array of spline points was not generated correctly.");
}

void insertData(size_t numRecords, uint64_t startingKey, int64_t startingData, uint64_t keyIncrement, int64_t dataIncrement) {
    for (size_t i = 0; i < numRecords; i++) {
        embedDBPut(state, &startingKey, &startingData);
        startingKey += keyIncrement;
        startingData += dataIncrement;
    }
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(spline_adds_points_with_consistent_slope_keys);
    RUN_TEST(spline_adds_points_varied_slope_keys);
    RUN_TEST(spline_cleans_correctly_with_consistent_slope_keys);
    RUN_TEST(spline_cleans_correctly_with_changing_slope_keys);
    return UNITY_END();
}
