#include <math.h>
#include <string.h>

#include "../src/sbits/sbits.h"
#include "../src/sbits/utilityFunctions.h"
#include "unity.h"

sbitsState *state;

typedef struct Node {
    int32_t key;
    void *data;
    uint32_t length;
    struct Node *next;
} Node;

void setUp(void) {
    state = (sbitsState *)malloc(sizeof(sbitsState));
    state->keySize = 4;
    state->dataSize = 4;
    state->pageSize = 512;
    state->bufferSizeInBlocks = 6;
    state->buffer = calloc(1, state->pageSize * state->bufferSizeInBlocks);
    state->startAddress = 0;
    state->endAddress = 65 * state->pageSize;
    state->eraseSizeInPages = 4;
    state->varAddressStart = state->endAddress;
    state->varAddressEnd = state->varAddressStart + 60 * state->pageSize;
    state->bitmapSize = 0;
    state->parameters = SBITS_RESET_DATA | SBITS_USE_VDATA;
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    int8_t result = sbitsInit(state, 1);
    TEST_ASSERT_EQUAL_INT8_MESSAGE(0, result, "SBITS did not initialize correctly.");
}

uint32_t randomData(void **data, uint32_t sizeLowerBound, uint32_t sizeUpperBound) {
    uint32_t size = rand() % (sizeUpperBound - sizeLowerBound) + sizeLowerBound;
    *data = malloc(size);
    for (uint32_t i = 0; i < size; i++) {
        *((uint8_t *)(*data) + i) = rand() % UINT8_MAX;
    }
    return size;
}

void randomVarData(uint32_t chance, uint32_t sizeLowerBound, uint32_t sizeUpperBound, uint8_t *usingVarData, uint32_t *length, void **varData) {
    *usingVarData = (rand() % chance) == 0;
    if (usingVarData) {
        *length = randomData(varData, sizeLowerBound, sizeUpperBound);
    } else {
        *length = 0;
        *varData = NULL;
    }
}

void insertRecords(int32_t numberOfRecords, int32_t startingKey, int32_t startingData, uint32_t chaneOfVariableData, uint32_t lowerVariableDataSizeBound, uint32_t upperVariableDataSizeBound, Node **linkedList) {
    int32_t key = startingKey;
    int32_t data = startingData;
    int8_t *recordBuffer = (int8_t *)calloc(1, state->recordSize);
    *linkedList = malloc(sizeof(Node));
    Node *linkedListTail = *linkedList;
    for (int32_t i = 0; i < numberOfRecords; i++) {
        void *variableData = NULL;
        uint8_t hasVarData = 0;
        uint32_t length;
        randomVarData(chaneOfVariableData, lowerVariableDataSizeBound, upperVariableDataSizeBound, &hasVarData, &length, &variableData);
        *((int32_t *)recordBuffer) += 1;
        *((int32_t *)(recordBuffer + state->keySize)) += 1;
        int8_t insertResult = sbitsPutVar(state, recordBuffer, (void *)(recordBuffer + state->keySize), hasVarData ? variableData : NULL, length);
        TEST_ASSERT_EQUAL_INT8_MESSAGE(0, insertResult, "SBITS failed to insert data.");
        linkedListTail->key = i;
        linkedListTail->data = variableData;
        linkedListTail->length = length;
        linkedListTail->next = malloc(sizeof(Node));
        linkedListTail = linkedListTail->next;
        linkedListTail->length = 0;
    }
}

void sbits_variable_data_page_numbers_are_correct() {
    Node *linkedList;
    insertRecords(1429, 1444, 64, 1, 10, 20, &linkedList);
    /* Number of records * average data size % page size */
    uint32_t numberOfPagesExpected = 53;
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(numberOfPagesExpected - 1, state->nextVarPageLogicalId, "SBITS next variable data logical page number is incorrect.");
    uint32_t pageNumber;
    printf("Number of pages expected: %i\n", numberOfPagesExpected);
    void *buffer = (int8_t *)state->buffer + state->pageSize * SBITS_VAR_READ_BUFFER(state->parameters);
    for (uint32_t i = 0; i < numberOfPagesExpected - 1; i++) {
        readVariablePage(state, i);
        memcpy(&pageNumber, buffer, sizeof(id_t));
        TEST_ASSERT_EQUAL_UINT32_MESSAGE(i, pageNumber, "SBITS variable data did not have the correct page number.");
    }
}

void tearDown(void) {
    sbitsClose(state);
    free(state);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(sbits_variable_data_page_numbers_are_correct);
    return UNITY_END();
}
