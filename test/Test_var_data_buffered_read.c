#include <stdio.h>

#include "../Unity/src/unity.h"
#include "../src/embedDB/embedDB.h"
#include "../src/embedDB/utilityFunctions.h"

// global variable for state. Use in setUp() function and tearDown()
embedDBState *state;

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
    state->numVarPages = 1000;
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
}

void tearDown() {
    printf("we got here\n");
    embedDBClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
    tearDownFile(state->varFile);
    free(state->buffer);
    free(state->fileInterface);
    free(state);
    state = NULL;
}

void test_keyInWriteBuffer_0_after_flush(void) {
    uint32_t key = 1;
    uint32_t data = 12345;
}

void test_var_insert_retrieval_no_flush(void) {
    uint32_t key = 1;
    uint32_t data = 12345;
    char varData[] = "Hello world";  // size 12
    embedDBPutVar(state, &key, &data, varData, 12);
    // embedDBFlush(state);
    //  reset data
    uint32_t peanuts = 0;
    // create var data stream
    embedDBVarDataStream *varStream = NULL;
    // create buffer for input
    uint32_t varBufSize = 12;  // Choose any size
    void *varDataBuffer = malloc(varBufSize);
    // query embedDB
    // embedDBGetVar(state, &key, &peanuts, &varStream);

    printf("%d\n", state->keyInWriteBuffer);

    /*
    if (varStream != NULL) {
        uint32_t bytesRead;
        while ((bytesRead = embedDBVarDataStreamRead(state, varStream, varDataBuf, varBufSize)) > 0) {
            // Process data in varDataBuf
        }
        free(varStream);
        varStream = NULL;
    }*/
}

// @TODO need to test range of inserts and retrieval
int main() {
    UNITY_BEGIN();
    RUN_TEST(test_var_insert_retrieval_no_flush);
    UNITY_END();
}
