#include <stdio.h>

#include "../Unity/src/unity.h"
#include "../src/embedDB/embedDB.h"
#include "../src/embedDB/utilityFunctions.h"

int insert_static_record(embedDBState* state, uint32_t key, uint32_t data);
void* query_record(embedDBState* state, uint32_t* key);
embedDBState* init_state();

// global variable for state. Use in setUp() function and tearDown()
embedDBState* state;

void setUp(void) {
    state = init_state();
}

void tearDown(void) {
    embedDBClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
    free(state->fileInterface);
    free(state->buffer);
    free(state);
    state = NULL;
}

// test saving data to buffer, flush, and then retrieval.
void test_single_insert_one_retrieval_flush(void) {
    // create a key
    uint32_t key = 1;
    // save to buffer
    insert_static_record(state, key, 123);
    // flush to file storage
    embedDBFlush(state);
    // query data
    int* return_data = query_record(state, &key);
    // test
    TEST_ASSERT_EQUAL(123, *return_data);
    // free allocated memory
    free(return_data);
}

void test_multiple_insert_one_retrieval_flush(void) {
    int numInserts = 100;
    for (int i = 0; i < numInserts; ++i) {
        insert_static_record(state, i, (i + 100));
    }
    embedDBFlush(state);
    uint32_t key = 35;
    int* return_data = query_record(state, &key);
    TEST_ASSERT_EQUAL(135, *return_data);
}

// test saving data to buffer and retrieves it
void test_single_insert_one_retrieval_no_flush(void) {
    // create a key
    uint32_t key = 1;
    // save to buffer
    insert_static_record(state, key, 123);
    // query data
    int* return_data = query_record(state, &key);
    // test
    if (return_data != NULL)
        TEST_ASSERT_EQUAL(123, *return_data);
    else
        TEST_FAIL_MESSAGE("If not marry, cry");
    // free allocated memory
    free(return_data);
}

// insert 5 records into database and retrieve one
void test_multiple_insert_one_retrieval_no_flush(void) {
    int numInserts = 5;
    for (int i = 0; i < numInserts; ++i) {
        insert_static_record(state, i, (i + 100));
    }

    uint32_t key = 3;
    int* return_data = query_record(state, &key);
    TEST_ASSERT_EQUAL(103, *return_data);
}

// test insert key, flush, and insert again for retrieval
void test_insert_flush_insert_buffer(void) {
    // create a key
    uint32_t key = 1;
    // save to buffer
    insert_static_record(state, key, 154);
    // query data
    int* return_data = query_record(state, &key);
    // flush
    embedDBFlush(state);
    key = 2;
    insert_static_record(state, key, 12345);
    return_data = query_record(state, &key);
    TEST_ASSERT_EQUAL(12345, *return_data);
}

// test checks to see if queried key > maxKey in buffer
void test_above_max_query(void) {
    // flush database to ensure nextDataPageId is > 0
    embedDBFlush(state);
    // insert random records
    int numInserts = 8;
    for (int i = 0; i < numInserts; ++i) {
        insert_static_record(state, i, (i + 100));
    }
    // query for max key not in database
    int key = 55;
    int* return_data = query_record(state, &key);
    // test if value is null
    TEST_ASSERT_EQUAL(NULL, return_data);
    // free
    free(return_data);
}

// test checks to see if queried key is >= the minKey in the buffer.
void test_flush_before_insert(void) {
    // flush database to ensure nextDataPageId is > 0
    embedDBFlush(state);
    // create a key
    uint32_t key = 1;
    // save to buffer
    insert_static_record(state, key, 123);
    // query data
    int* return_data = query_record(state, &key);
    // test
    TEST_ASSERT_EQUAL(123, *return_data);
    // free allocated memory
    free(return_data);
}

// test checks retrievel if there is no data and nothing in the buffer
void test_no_data(void) {
    // create a key
    uint32_t key = 1;
    // allocate dataSize record in heap
    void* temp = calloc(1, state->dataSize);
    // query embedDB and returun pointer
    int8_t status = embedDBGet(state, &key, (void*)temp);
    // test
    TEST_ASSERT_EQUAL(-1, status);
    free(temp);
}

int main() {
    UNITY_BEGIN();
    RUN_TEST(test_single_insert_one_retrieval_flush);
    RUN_TEST(test_multiple_insert_one_retrieval_flush);
    RUN_TEST(test_single_insert_one_retrieval_no_flush);
    RUN_TEST(test_multiple_insert_one_retrieval_no_flush);
    RUN_TEST(test_insert_flush_insert_buffer);
    RUN_TEST(test_above_max_query);
    RUN_TEST(test_flush_before_insert);
    RUN_TEST(test_no_data);
    UNITY_END();
}

/* function puts a static record into buffer without flushing. Creates and frees record allocation in the heap.*/
int insert_static_record(embedDBState* state, uint32_t key, uint32_t data) {
    // calloc dataSize bytes in heap.
    void* dataPtr = calloc(1, state->dataSize);
    // set dataPtr[0] to data
    ((uint32_t*)dataPtr)[0] = data;
    // insert into buffer, save result
    char result = embedDBPut(state, (void*)&key, (void*)dataPtr);
    // free dataPtr
    free(dataPtr);
    // return based on success
    return (result == 0) ? 0 : -1;
}

/* function that returns a pointer to allocated space in heap
   Calling function must free memory as appropriate          */
// @TODO make this into a malloc wrapper so it's clear that I need to clear memory.
void* query_record(embedDBState* state, uint32_t* key) {
    // allocate dataSize record in heap
    void* temp = calloc(1, state->dataSize);
    // query embedDB and returun pointer
    if (embedDBGet(state, key, (void*)temp) == 0) return temp;
    // else, return NULL
    return 0;
}

/* Function returns a pointer to a newly created embedDBState*/
/* @TODO: Make this more dynamic?*/
embedDBState* init_state() {
    embedDBState* state = (embedDBState*)malloc(sizeof(embedDBState));
    if (state == NULL) {
        printf("Unable to allocate state. Exiting\n");
        exit(0);
    }
    // configure state variables
    state->recordSize = 16;  // size of record in bytes
    state->keySize = 4;      // size of key in bytes
    state->dataSize = 12;    // size of data in bytes
    state->pageSize = 512;   // page size (I am sure this is in bytes)
    state->numSplinePoints = 300;
    state->bitmapSize = 1;
    state->bufferSizeInBlocks = 4;  // size of the buffer in blocks (where I am assuming that a block is the same as a page size)
    // allocate buffer
    state->buffer = malloc((size_t)state->bufferSizeInBlocks * state->pageSize);
    // check
    if (state->buffer == NULL) {
        printf("Unable to allocate buffer. Exciting\n");
        exit(0);
    }
    // address level parameters
    state->numDataPages = 20000;
    state->numIndexPages = 48;
    state->eraseSizeInPages = 4;
    // configure file interface
    char dataPath[] = "build/artifacts/dataFile.bin", indexPath[] = "build/artifacts/indexFile.bin", varPath[] = "build/artifacts/varFile.bin";
    state->fileInterface = getFileInterface();
    state->dataFile = setupFile(dataPath);
    state->indexFile = setupFile(indexPath);
    // configure state
    state->parameters = EMBEDDB_USE_BMAP | EMBEDDB_USE_INDEX | EMBEDDB_RESET_DATA;
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
    return state;
}
