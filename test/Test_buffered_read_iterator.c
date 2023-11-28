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

// test ensures iterator checks written pages using keys after writing.
void test_iterator_flush_on_keys_int(void) {
    // create key and data
    uint32_t key = 1;
    uint32_t data = 111;
    int recNum = 36;
    // inserting records
    for (int i = 0; i < recNum; ++i) {
        insert_static_record(state, key, data);
        key += 1;
        data += 5;
    }
    // setup iterator
    embedDBIterator it;
    uint32_t itKey = 0;
    uint32_t itData[] = {0, 0, 0};
    uint32_t minKey = 1, maxKey = 36;
    it.minKey = &minKey;
    it.maxKey = &maxKey;
    it.minData = NULL;
    it.maxData = NULL;

    int data_comparison = 111;

    embedDBInitIterator(state, &it);

    // test data
    while (embedDBNext(state, &it, (void**)&itKey, (void**)&itData)) {
        TEST_ASSERT_EQUAL(data_comparison, *(int*)itData);
        data_comparison += 5;
    }

    // close
    embedDBCloseIterator(&it);
}

// test ensures iterator checks written pages using keys after writing.
void test_iterator_flush_on_keys_float(void) {
    // create key and data
    float key = 1;
    float data = 111.00;
    int recNum = 36;
    // inserting records
    for (int i = 0; i < recNum; ++i) {
        insert_static_record(state, key, data);
        key += 1;
        data += 5;
    }
    // setup iterator
    embedDBIterator it;
    float itKey = 0;
    float itData[] = {0, 0, 0};
    uint32_t minKey = 1, maxKey = 36;
    it.minKey = &minKey;
    it.maxKey = &maxKey;
    it.minData = NULL;
    it.maxData = NULL;

    float data_comparison = 111.00;

    embedDBInitIterator(state, &it);

    // test data
    while (embedDBNext(state, &it, (void**)&itKey, (void**)&itData)) {
        TEST_ASSERT_EQUAL(data_comparison, *(int*)itData);
        data_comparison += 5;
    }

    // close
    embedDBCloseIterator(&it);
}

// test ensures iterator checks write buffer using keys without writing to file storage.
void test_iterator_no_flush_on_keys(void) {
    // create key and data
    uint32_t key = 1;
    uint32_t data = 111;
    // records all to be contained in buffer
    int recNum = 16;
    // inserting records
    for (int i = 0; i < recNum; ++i) {
        insert_static_record(state, key, data);
        key += 1;
        data += 5;
    }
    // setup iterator
    embedDBIterator it;
    uint32_t *itKey, *itData;
    key = 1;
    data = 111;

    uint32_t minKey = 1, maxKey = 15;
    it.minKey = &minKey;
    it.maxKey = &maxKey;
    it.minData = NULL;
    it.maxData = NULL;

    embedDBInitIterator(state, &it);
    // test data
    while (embedDBNext(state, &it, &itKey, &itData)) {
        TEST_ASSERT_EQUAL(data, itData);
        data += 5;
    }
    // close
    embedDBCloseIterator(&it);
}

// test ensures iterator checks written pages using data after writing.
void test_iterator_flush_on_data(void) {
    // create key and data
    uint32_t key = 1;
    uint32_t data = 111;
    int recNum = 36;
    // inserting records
    for (int i = 0; i < recNum; ++i) {
        insert_static_record(state, key, data);
        key += 1;
        data += 5;
    }
    // setup iterator
    embedDBIterator it;
    uint32_t itKey = NULL;
    uint32_t itData[] = {0, 0, 0};
    it.minKey = NULL;
    it.maxKey = NULL;
    uint32_t minData = 111, maxData = 286;
    it.minData = &minData;
    it.maxData = &maxData;

    int key_comparison = 1;

    embedDBInitIterator(state, &it);

    // test data
    while (embedDBNext(state, &it, (void**)&itKey, (void**)&itData)) {
        TEST_ASSERT_EQUAL(key_comparison, itKey);
        key_comparison += 1;
    }

    // close
    embedDBCloseIterator(&it);
}

// test ensures iterator checks written pages using data without flushing to storage
void test_iterator_no_flush_on_data(void) {
    // create key and data
    uint32_t key = 1;
    uint32_t data = 111;
    int recNum = 15;
    // inserting records
    for (int i = 0; i < recNum; ++i) {
        insert_static_record(state, key, data);
        key += 1;
        data += 5;
    }
    // setup iterator
    embedDBIterator it;
    uint32_t itKey = NULL;
    uint32_t itData[] = {0, 0, 0};
    it.minKey = NULL;
    it.maxKey = NULL;
    uint32_t minData = 111, maxData = 186;
    it.minData = &minData;
    it.maxData = &maxData;

    int key_comparison = 1;

    embedDBInitIterator(state, &it);

    // test data
    while (embedDBNext(state, &it, (void**)&itKey, (void**)&itData)) {
        TEST_ASSERT_EQUAL(key_comparison, itKey);
        key_comparison += 1;
    }

    // close
    embedDBCloseIterator(&it);
}

// will need a test for defined minKey (that is so it can use the bitmap and spline etc)
// will need a test for inserting records, flushing, and then inserting more (think getting nextDataPageId up to satisfy this if statement if (it->nextDataPage >= state->nextDataPageId))
// will need a test for a variety of data types including floats
// will need to have a test where some of the records are flushed to storage and some of them are in the buffer. I can imagine this is a primary use-case
// NULL case?

int main() {
    UNITY_BEGIN();
    RUN_TEST(test_iterator_flush_on_keys_int);
    RUN_TEST(test_iterator_flush_on_keys_float);
    RUN_TEST(test_iterator_no_flush_on_keys);
    RUN_TEST(test_iterator_flush_on_data);
    RUN_TEST(test_iterator_no_flush_on_data);
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
