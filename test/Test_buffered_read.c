#include <stdio.h>
#include "../src/embedDB/embedDB.h"
#include "../src/embedDB/utilityFunctions.h"
// I don't love how I'm including the unity header right now...
#include "../Unity/src/unity.h"

int insert_static_record(embedDBState* state, uint32_t key, uint32_t data);
void* query_record(embedDBState* state, uint32_t* key);
embedDBState* init_state();

// global variable for state. Use in setUp() function and tearDown()
embedDBState* state;

// Once I figure out how to read from the buffer, I also need to test if we change the key to a different value if that effects the buffer as well.
// what I'm saying is, since we are passing by reference, if I modify the original key value, will that affect my retrieval? 

void setUp(void) {
    state = init_state();
}

void tearDown(void) {
    embedDBClose(state);
    tearDownFile(state->dataFile);
    tearDownFile(state->indexFile);
    //tearDownFile(state->varFile);   // seg fualted here. Will need to implement when I haft to test var file queries. 
    free(state->fileInterface);
    free(state->buffer);
    free(state);
    }

// test saving data to buffer, flush, and then retrieval. 
void test_single_insert_one_retrieval_flush(void) {
    // create a key 
    uint32_t key = 1; 
    // save to buffer
    insert_static_record(state, key, 123);
    // flush to file storage
    embedDBFlush(state);
    //query data
    int* return_data = query_record(state, &key);
    // test 
    TEST_ASSERT_EQUAL(123, *return_data);
    // free allocated memory
    free(return_data);
    
}

void test_multiple_insert_one_retrieval_flush(void){
    int numInserts = 100; 
    for(int i = 0; i < numInserts; ++i){
        insert_static_record(state, i, (i+100));
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
    //query data
    int* return_data = query_record(state, &key);
    // test 
    TEST_ASSERT_EQUAL(123, *return_data);
    // free allocated memory
    free(return_data);
}

void test_multiple_insert_one_retrieval_no_flush(void){
    int numInserts = 100; 
    for(int i = 0; i < numInserts; ++i){
        insert_static_record(state, i, (i+100));
    }
    uint32_t key = 35;
    int* return_data = query_record(state, &key);
    TEST_ASSERT_EQUAL(135, *return_data);
}

void test_insert_and_write_no_flush(void){
    // create a key 
    uint32_t key = 1; 
    // save to buffer
    insert_static_record(state, key, 154);
    // query data 
    int* return_data = query_record(state, &key);
    // test
    TEST_ASSERT_EQUAL(154, *return_data);
    // save again 
    key = 2; 
    insert_static_record(state, key, 321);
    // query data 
    return_data = query_record(state, &key);
    // test
    TEST_ASSERT_EQUAL(321, *return_data);
    // free allocated 
    free(return_data); 
}

int main(){
    UNITY_BEGIN();
    RUN_TEST(test_single_insert_one_retrieval_flush);
    RUN_TEST(test_multiple_insert_one_retrieval_flush); // this test seg faults for me using modified binary search. 
    RUN_TEST(test_single_insert_one_retrieval_no_flush);
    RUN_TEST(test_multiple_insert_one_retrieval_no_flush);
    RUN_TEST(test_insert_and_write_no_flush);
}

/* function puts a static record into buffer without flushing. Creates and frees record allocation in the heap.*/
int insert_static_record(embedDBState* state, uint32_t key, uint32_t data){
    // calloc dataSize bytes in heap. 
    void* dataPtr = calloc(1, state->dataSize);
    // set dataPtr[0] to data 
    ((uint32_t*)dataPtr)[0]= data;
    // insert into buffer, save result 
    char result = embedDBPut(state, (void*) &key, (void*) dataPtr);
    // free dataPtr
    free(dataPtr);
    // return based on success
    return (result == 0) ? 0 : -1;
}

/* function that returns a pointer to allocated space in heap */
void* query_record(embedDBState* state, uint32_t* key){
    // allocate dataSize record in heap
    void* temp = calloc(1, state->dataSize);
    // query embedDB
    embedDBGet(state, key, (void*) temp);
    // return pointer 
    return temp;
}

/* Function returns a pointer to a newly created embedDBState*/
/* @TODO: Make this more dynamic?*/
embedDBState* init_state(){
    embedDBState* state = (embedDBState *)malloc(sizeof(embedDBState));
    if(state == NULL){
        printf("Unable to allocate state. Exiting\n");
        exit(0);
    }
     // configure state variables
    state->recordSize = 16; 
    state->keySize = 4; 
    state->dataSize = 12; 
    state->pageSize = 512; 
    state->numSplinePoints = 300;
    state->bitmapSize = 1;
    state->bufferSizeInBlocks = 4; 
    // allocate buffer 
    state->buffer = malloc((size_t) state->bufferSizeInBlocks * state->pageSize);
    // check 
    if(state->buffer == NULL){
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
    
    if(embedDBInit(state, splineMaxError) != 0) {
        printf("Initialization error");
        exit(0);
    }
    return state;
}
