#include <stdio.h>

#include "../Unity/src/unity.h"
#include "../src/embedDB/embedDB.h"
#include "../src/embedDB/utilityFunctions.h"

// global variable for state. Use in setUp() function and tearDown()
embedDBState *state;
int inserted;

void setUp(void) {
    state = (embedDBState *)malloc(sizeof(embedDBState));
    if (state == NULL) {
        printf("Unable to allocate state. Exiting\n");
        exit(0);
    }
    // configure state variables
    state->keySize = 4;
    state->dataSize = 8;
    state->pageSize = 512;
    state->numSplinePoints = 300;
    state->bitmapSize = 1;
    state->bufferSizeInBlocks = 6;
    // allocate buffer
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    // check
    if (state->buffer == NULL) {
        printf("Unable to allocate buffer. Exciting\n");
        exit(0);
    }
    // address level parameters
    state->numDataPages = 1000;
    state->numIndexPages = 48;
    state->numVarPages = 75;
    state->eraseSizeInPages = 4;
    // configure file interface
    char dataPath[] = "build/artifacts/dataFile.bin", indexPath[] = "build/artifacts/indexFile.bin", varPath[] = "build/artifacts/varFile.bin";
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);
    state->varFile = setupFile(varPath);
    // configure state
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_USE_VDATA | EMBEDDB_RESET_DATA;
    // Setup for data and bitmap comparison functions */
    state->inBitmap = inBitmapInt8;
    state->updateBitmap = updateBitmapInt8;
    state->buildBitmapFromRange = buildBitmapInt8FromRange;
    state->compareKey = int32Comparator;
    state->compareData = int32Comparator;
    // init
    size_t splineMaxError = 1;

    if (embedDBInit(state, splineMaxError) != 0) {
        printf("Initialization error");
        exit(0);
    }

    embedDBResetStats(state);

    // init inserted
    int inserted = 0;
}

void tearDown() {
    embedDBClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
    tearDownFile(state->varFile);
    free(state->buffer);
    free(state->fileInterface);
    free(state);
    state = NULL;
    //inserted = 0;
}

void test_insert_single_record_and_retrieval_from_buffer_no_flush(void) {
    uint32_t key = 121;
    uint32_t data = 12345;
    char varData[] = "Hello world";  // size 12
    embedDBPutVar(state, &key, &data, varData, 12);
    //  reset data
    uint32_t expData[] = {0,0,0};
    // create var data stream
    embedDBVarDataStream *varStream = NULL;
    // create buffer for input
    uint32_t varBufSize = 12;  // Choose any size
    void *varDataBuffer = malloc(varBufSize);
    // query embedDB
    int r = embedDBGetVar(state, &key, &expData, &varStream);
    TEST_ASSERT_EQUAL_INT_MESSAGE(r, 0, "Records should have been found.");
    TEST_ASSERT_EQUAL_INT(*expData, data);
    uint32_t bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(12, bytesRead, "Returned vardata was not the right length");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, 12, "embedDBGetVar did not return the correct vardata");
}

void test_single_variable_page_insert_and_retrieve_from_buffer(void){
    // insert just over a page worth's of data
    insertRecords(27);
    int key = 26;
    uint32_t expData[] = {0,0,0};
    // create var data stream
    embedDBVarDataStream *varStream = NULL;
    // create buffer for input
    uint32_t varBufSize = 15;  // Choose any size
    void *varDataBuffer = malloc(varBufSize);
    // query embedDB
    int r = embedDBGetVar(state, &key, &expData, &varStream);
    TEST_ASSERT_EQUAL_INT_MESSAGE(r, 0, "Records should have been found.");
    uint32_t bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    char varData[] = "Testing 026...";
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, bytesRead, "Returned vardata was not the right length");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, 15, "embedDBGetVar did not return the correct vardata");
}

/*
void test_insert_retrieve_insert_and_retrieve_again(void){
    // insert 3 records 
    insertRecords(3);
    // retrieve record 
    int key = 2; 
    uint32_t expData[] = {0,0,0};
     // create var data stream
    embedDBVarDataStream *varStream = NULL;
    // create buffer for input
    uint32_t varBufSize = 15;  // Choose any size
    void *varDataBuffer = malloc(varBufSize);
    // query embedDB
    int r = embedDBGetVar(state, &key, &expData, &varStream);
    TEST_ASSERT_EQUAL_INT_MESSAGE(r, 0, "Records should have been found.");
    uint32_t bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize);
    char varData[] = "Testing 002...";
    TEST_ASSERT_EQUAL_UINT32_MESSAGE(15, bytesRead, "Returned vardata was not the right length");
    TEST_ASSERT_EQUAL_CHAR_ARRAY_MESSAGE(varData, varDataBuffer, 15, "embedDBGetVar did not return the correct vardata");
    free(varDataBuffer);
    insertRecords(60);
    key = 25;
    varDataBuffer = malloc(varBufSize);
    r = embedDBGetVar(state, &key, &expData, &varStream);
    TEST_ASSERT_EQUAL_INT_MESSAGE(r, 0, "Records should have been found.");
}*/

void test_var_read_iterator_buffer(void){
    insertRecords(3);
    printf("next datapage id = %d\n", state->nextDataPageId);
    embedDBIterator it;
    uint32_t *itKey, *itData;
    uint32_t minKey = 0, maxKey = 2;
    it.minKey = &minKey;
    it.maxKey = &maxKey;
    it.minData = NULL;
    it.maxData = NULL;

    embedDBVarDataStream *varStream = NULL;
    uint32_t varBufSize = 15;  // Choose any size
    void *varDataBuffer = malloc(varBufSize);

    embedDBInitIterator(state, &it);

    while (embedDBNextVar(state, &it, &itKey, itData, &varStream)) {
        printf("This was never called\n");
        /* Process fixed part of record */
        /* Process vardata if this record has it */
        if (varStream != NULL) {
            uint32_t numBytesRead = 0;
            while ((numBytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuffer, varBufSize)) > 0) {
                /* Process the data read into the buffer */
                for (uint32_t i = 0; i < numBytesRead; i++) {
                    printf("%02X ", (uint8_t*)varDataBuffer + i);
                }
            }
            printf("\n");

            free(varStream);
            varStream = NULL;
        }
    }

    free(varDataBuffer);
    embedDBCloseIterator(&it);

}


int insertRecords(uint32_t n) {
    char varData[] = "Testing 000...";
    int targetNum = n + inserted;
    for (uint64_t i = inserted; i < targetNum; i++) {
        varData[10] = (char)(i % 10) + '0';
        varData[9] = (char)((i / 10) % 10) + '0';
        varData[8] = (char)((i / 100) % 10) + '0';

        uint64_t data = i % 100;
        int result = embedDBPutVar(state, &i, &data, varData, 15);
        if (result != 0) {
            return result;
        }
    }
    return 0;
    inserted++;
}

// @TODO I want to know what happens with these cases: 
// - If the variable record takes up multiple pages 

int main() {
    UNITY_BEGIN();
    //RUN_TEST(test_insert_single_record_and_retrieval_from_buffer_no_flush);
    //RUN_TEST(test_single_variable_page_insert_and_retrieve_from_buffer);
    //RUN_TEST(test_insert_retrieve_insert_and_retrieve_again);
    RUN_TEST(test_var_read_iterator_buffer);
    UNITY_END();
}
