#include <errno.h>
#include <string.h>
#include <time.h>

#include "sbits.h"

#define NUM_STEPS 10
#define NUM_RUNS 1

void runalltests_sbits_variable_data();
void updateBitmapInt8Bucket(void *data, void *bm);
void buildBitmapInt8BucketWithRange(void *min, void *max, void *bm);
int8_t inBitmapInt8Bucket(void *data, void *bm);
void updateBitmapInt16(void *data, void *bm);
int8_t inBitmapInt16(void *data, void *bm);
void updateBitmapInt64(void *data, void *bm);
int8_t inBitmapInt64(void *data, void *bm);
int8_t int32Comparator(void *a, void *b);
uint32_t keyModifier(uint32_t inputKey);
uint32_t randomData(void **data, uint32_t sizeLowerBound,
                    uint32_t sizeUpperBound);

int main() { runalltests_sbits_variable_data(); }

void runalltests_sbits_variable_data() {
    printf("\nSTARTING SBITS VARIABLE DATA TESTS.\n");
    // Two extra bufferes required for variable data
    int8_t M = 6;

    // Initialize to default values
    int32_t numRecords = 2000;   // default values
    int32_t testRecords = 2000;  // default values
    uint8_t useRandom = 0;         // default values
    size_t splineMaxError = 0;     // default values
    uint32_t stepSize = numRecords / NUM_STEPS;
    count_t r, l;
    uint32_t times[NUM_STEPS][NUM_RUNS];
    uint32_t reads[NUM_STEPS][NUM_RUNS];
    uint32_t writes[NUM_STEPS][NUM_RUNS];
    uint32_t overwrites[NUM_STEPS][NUM_RUNS];
    uint32_t hits[NUM_STEPS][NUM_RUNS];
    uint32_t rtimes[NUM_STEPS][NUM_RUNS];
    uint32_t rreads[NUM_STEPS][NUM_RUNS];
    uint32_t rhits[NUM_STEPS][NUM_RUNS];
    int8_t seqdata = 1;
    FILE *infile = NULL, *infileRandom = NULL;
    uint32_t minRange, maxRange;

    if (seqdata != 1) { /* Open file to read input records */

        // measure1_smartphone_sens.bin
        // infile = fopen("data/measure1_smartphone_sens.bin", "r+b");
        // infileRandom = fopen("data/measure1_smartphone_sens_randomized.bin",
        // "r+b"); minRange = 0; maxRange = INT32_MAX; numRecords = 18354;
        // testRecords = 18354;

        // position.bin
        // infile = fopen("data/position.bin", "r+b");
        // infileRandom = fopen("data/position_randomized.bin", "r+b");
        // minRange = 0;
        // maxRange = INT32_MAX;
        // numRecords = 1518;
        // testRecords = 1518;

        // ethylene_CO.bin
        // infile = fopen("data/ethylene_CO.bin", "r+b");
        // infileRandom = fopen("data/ethylene_CO_randomized.bin", "r+b");
        // minRange = 0;
        // maxRange = INT32_MAX;
        // numRecords = 4085589;
        // testRecords = 4085589;

        // Watch_gyroscope.bin
        // infile = fopen("data/Watch_gyroscope.bin", "r+b");
        // infileRandom = fopen("data/Watch_gyroscope_randomized.bin", "r+b");
        // minRange = 0;
        // maxRange = INT32_MAX;
        // numRecords = 2865713;
        // testRecords = 2865713;

        // PRSA_Data_Hongxin.bin
        // infile = fopen("data/PRSA_Data_Hongxin.bin", "r+b");
        // infileRandom = fopen("data/PRSA_Data_Hongxin_randomized.bin", "r+b");
        // minRange = 0;
        // maxRange = INT32_MAX;
        // numRecords = 35064;
        // testRecords = 35064;

        // S7hl500K.bin
        // infile = fopen("data/S7hl500K.bin", "r+b");
        // minRange = 0;
        // maxRange = INT32_MAX;
        // numRecords = 500000;

        // infile = fopen("data/seatac_data_100KSorted.bin", "r+b");
        // infileRandom = fopen("data/seatac_data_100KSorted_randomized.bin",
        // "r+b"); minRange = 1314604380; maxRange = 1609487580; numRecords =
        // 100001; testRecords = 100001;

        // infile = fopen("data/uwa500K.bin", "r+b");
        // infileRandom =
        // fopen("data/uwa_data_only_2000_500KSorted_randomized.bin", "r+b");
        // minRange = 946713600;
        // maxRange = 977144040;
        // numRecords = 500000;
        // testRecords = 500000;

        splineMaxError = 1;
        useRandom = 0;

        stepSize = numRecords / NUM_STEPS;
    }

    for (r = 0; r < NUM_RUNS; r++) {
        sbitsState *state = (sbitsState *)malloc(sizeof(sbitsState));

        state->recordSize = 20;
        state->keySize = 4;
        state->dataSize = 12;
        state->pageSize = 512;
        state->bitmapSize = 0;
        state->bufferSizeInBlocks = M;
        state->buffer =
            malloc((size_t)state->bufferSizeInBlocks * state->pageSize);

        /* Address level parameters */
        state->startAddress = 0;
        state->endAddress = state->pageSize * numRecords / 10;
        state->varAddressStart = 0;
        state->varAddressEnd = state->pageSize * numRecords / 10;
        state->eraseSizeInPages = 4;

        state->parameters = SBITS_USE_BMAP | SBITS_USE_INDEX | SBITS_USE_VDATA;

        if (SBITS_USING_INDEX(state->parameters) == 1)
            state->endAddress +=
                state->pageSize * (state->eraseSizeInPages * 2);
        if (SBITS_USING_BMAP(state->parameters)) state->bitmapSize = 8;

        /* Setup for data and bitmap comparison functions */
        state->inBitmap = inBitmapInt16;
        state->updateBitmap = updateBitmapInt16;
        state->inBitmap = inBitmapInt64;
        state->updateBitmap = updateBitmapInt64;
        state->compareKey = int32Comparator;
        state->compareData = int32Comparator;

        if (sbitsInit(state, splineMaxError) != 0) {
            printf("Initialization error.\n");
            return;
        } else {
            printf("Initialization success.\n");
        }

        // Initialize Buffer
        int8_t *recordBuffer = (int8_t *)calloc(1, state->recordSize);

        printf("\n\nINSERT TEST:\n");
        /* Insert records into structure */
        uint32_t start = clock();

        int32_t i;
        if (seqdata == 1) {
            for (i = 0; i < numRecords; i++) {
                *((int32_t *)recordBuffer) = i;
                *((int32_t *)(recordBuffer + 4)) = (i % 100);
                char *variableData = "Hello there";
                uint32_t length = strlen(variableData) + 1;
                // Put variable length data
                sbitsPutVar(state, recordBuffer, (void *)(recordBuffer + 4), i % 10 == 0 ? variableData : NULL, length);

                if (i % stepSize == 0) {
                    // printf("Num: %lu KEY: %lu\n", i, i);
                    l = i / stepSize - 1;
                    if (l < NUM_STEPS && l >= 0) {
                        times[l][r] =
                            ((clock() - start) * 1000) / CLOCKS_PER_SEC;
                        reads[l][r] = state->numReads;
                        writes[l][r] = state->numWrites;
                        overwrites[l][r] = 0;
                        hits[l][r] = state->bufferHits;
                    }
                }
            }
        } else { /* Read data from a file */
            char infileBuffer[512];
            int8_t headerSize = 16;
            int32_t i = 0;
            fseek(infile, 0, SEEK_SET);
            // uint32_t readCounter = 0;
            while (1) {
                /* Read page */
                if (0 == fread(infileBuffer, state->pageSize, 1, infile)) break;
                // readCounter++;
                /* Process all records on page */
                int16_t count = *((int16_t *)(infileBuffer + 4));
                for (int j = 0; j < count; j++) {
                    void *buf =
                        (infileBuffer + headerSize + j * state->recordSize);

                    // printf("Key: %lu, Data: %lu, Page num: %lu, i:
                    // %lu\n",
                    // *(id_t*)buf, *(id_t*)(buf + 4), i/31, i);
                    void *variableData = NULL;
                    uint32_t length = 0;
                    if (rand() % 100 > 70) {
                        length = randomData(&variableData, 0, 100);
                    }
                    // Put variable length data
                    sbitsPutVar(state, recordBuffer, (void *)(recordBuffer + 4),
                                variableData, length);
                    // if ( i < 100000)
                    //   printf("%lu %d %d %d\n", *((uint32_t*) buf),
                    //   *((int32_t*) (buf+4)), *((int32_t*) (buf+8)),
                    //   *((int32_t*) (buf+12)));

                    if (i % stepSize == 0) {
                        printf("Num: %lu KEY: %lu\n", i, *((int32_t *)buf));
                        l = i / stepSize - 1;
                        if (l < NUM_STEPS && l >= 0) {
                            times[l][r] =
                                ((clock() - start) * 1000) / CLOCKS_PER_SEC;
                            reads[l][r] = state->numReads;
                            writes[l][r] = state->numWrites;
                            overwrites[l][r] = 0;
                            hits[l][r] = state->bufferHits;
                        }
                    }
                    i++;
                    /* Allows stopping at set number of records instead of
                     * reading entire file */
                    if (i == numRecords) {
                        maxRange = *((uint32_t *)buf);
                        printf("Num: %lu KEY: %lu\n", i, *((int32_t *)buf));
                        goto doneread;
                    }
                }
            }
            numRecords = i;
        }

    doneread:

        sbitsFlush(state);
        fflush(state->file);
        uint32_t end = clock();

        l = NUM_STEPS - 1;
        times[l][r] = ((end - start) * 1000) / CLOCKS_PER_SEC;
        reads[l][r] = state->numReads;
        writes[l][r] = state->numWrites;
        overwrites[l][r] = 0;
        hits[l][r] = state->bufferHits;

        printf("Elapsed Time: %lu ms\n", times[l][r]);
        printf("Records inserted: %lu\n", numRecords);

        printStats(state);
        resetStats(state);

        printf("\n\nQUERY TEST:\n");
        /* Verify that all values can be found and test query performance */
        start = clock();

        if (seqdata == 1) {
            for (i = 0; i < numRecords; i++) {
                int32_t key = i;
                void* varData = NULL;
                int8_t result = sbitsGet(state, &key, recordBuffer);

                if (result == -1) printf("ERROR: Failed to find: %lu\n", key);
                if (seqdata == 1 && *((int32_t *)recordBuffer) != key % 100) {
                    printf("ERROR: Wrong data for: %lu\n", key);
                    printf("Key: %lu Data: %lu\n", key, *((int32_t *)recordBuffer));
                    return;
                }

                if (i % stepSize == 0) {
                    l = i / stepSize - 1;
                    if (l < NUM_STEPS && l >= 0) {
                        rtimes[l][r] = ((clock() - start) * 1000) / CLOCKS_PER_SEC;
                        rreads[l][r] = state->numReads;
                        rhits[l][r] = state->bufferHits;
                    }
                }
            }
        } else { /* Data from file */
            char infileBuffer[512];
            int8_t headerSize = 16;
            i = 0;
            int8_t queryType = 1;

            if (queryType == 1) { /* Query each record from original data set. */
                if (useRandom) {
                    fseek(infileRandom, 0, SEEK_SET);
                } else {
                    fseek(infile, 0, SEEK_SET);
                }
                int32_t readCounter = 0;
                while (1) {
                    /* Read page */
                    if (useRandom) {
                        if (0 == fread(infileBuffer, state->pageSize, 1,
                                       infileRandom))
                            break;
                    } else {
                        if (0 ==
                            fread(infileBuffer, state->pageSize, 1, infile))
                            break;
                    }

                    readCounter++;
                    /* Process all records on page */
                    int16_t count = *((int16_t *)(infileBuffer + 4));
                    for (int j = 0; j < count; j++) {
                        void *buf =
                            (infileBuffer + headerSize + j * state->recordSize);
                        int32_t *key = (int32_t *)buf;

                        int8_t result = sbitsGet(state, key, recordBuffer);
                        if (result != 0)
                            printf("ERROR: Failed to find key: %lu, i: %lu\n",
                                   *key, i);
                        if (*((int32_t *)recordBuffer) !=
                            *((int32_t *)((int8_t *)buf + 4))) {
                            printf(
                                "ERROR: Wrong data for: Key: %lu Data: %lu\n",
                                *key, *((int32_t *)recordBuffer));
                            printf("%lu %d %d %d\n", *((uint32_t *)buf),
                                   *((int32_t *)((int8_t *)buf + 4)),
                                   *((int32_t *)((int8_t *)buf + 8)),
                                   *((int32_t *)((int8_t *)buf + 12)));
                            result = sbitsGet(state, key, recordBuffer);
                            // return;
                        }

                        if (i % stepSize == 0) {
                            l = i / stepSize - 1;
                            printf("Num: %lu KEY: %lu\n", i, *key);
                            if (l < NUM_STEPS && l >= 0) {
                                rtimes[l][r] =
                                    ((clock() - start) * 1000) / CLOCKS_PER_SEC;
                                rreads[l][r] = state->numReads;
                                rhits[l][r] = state->bufferHits;
                            }
                        }
                        i++;
                        if (i == numRecords ||
                            i == testRecords) /* Allows ending test after set
                                                 number of records rather than
                                                 processing entire file */
                            goto donetest;
                    }
                }
            donetest:
                numRecords = i;
            } else if (queryType == 2) { /* Query random values in range. May
                                            not exist in data set. */
                i = 0;
                int32_t num = maxRange - minRange;
                printf("Rge: %d Rand max: %d\n", num, RAND_MAX);
                while (i < numRecords) {
                    double scaled =
                        ((double)rand() * (double)rand()) / RAND_MAX / RAND_MAX;
                    int32_t key = (num + 1) * scaled + minRange;

                    // printf("Key :%d\n", key);
                    if (i == 2) {
                        sbitsGet(state, &key, recordBuffer);
                    } else {
                        sbitsGet(state, &key, recordBuffer);
                    }

                    if (i % stepSize == 0) {
                        l = i / stepSize - 1;
                        printf("Num: %lu KEY: %lu\n", i, key);
                        if (l < NUM_STEPS && l >= 0) {
                            rtimes[l][r] =
                                ((clock() - start) * 1000) / CLOCKS_PER_SEC;
                            rreads[l][r] = state->numReads;
                            rhits[l][r] = state->bufferHits;
                        }
                    }
                    i++;
                }
            } else { /* Data value query for given value range */
                int8_t success = 1;
                int32_t *itKey, *itData;
                sbitsIterator it;
                it.minKey = NULL;
                it.maxKey = NULL;
                int32_t mv = 800;
                int32_t v = 1000;
                it.minData = &mv;
                it.maxData = &v;
                int32_t rec, reads;

                start = clock();
                mv = 280;
                // for (int i = 0; i < 1000; i++)
                // for (int i = 0; i < 16; i++)
                for (int i = 0; i < 65; i++)  // 65
                {
                    // mv = (rand() % 60 + 30) * 10;
                    // mv += 30;
                    mv += 10;
                    v = mv;

                    resetStats(state);
                    sbitsInitIterator(state, &it);
                    rec = 0;
                    reads = state->numReads;
                    // printf("Min: %d Max: %d\n", mv, v);
                    while (sbitsNext(state, &it, (void **)&itKey,
                                     (void **)&itData)) {
                        // printf("Key: %d  Data: %d\n", *itKey, *itData);
                        if (*((int32_t *)itData) < *((int32_t *)it.minData) ||
                            *((int32_t *)itData) > *((int32_t *)it.maxData)) {
                            success = 0;
                            printf("Key: %d Data: %d Error\n", *itKey, *itData);
                        }
                        rec++;
                    }
                    // printf("Read records: %d\n", rec);
                    // printStats(state);
                    printf(
                        "Num: %lu KEY: %lu Perc: %d Records: %d Reads: %d \n",
                        i, mv,
                        ((state->numReads - reads) * 1000 /
                         (state->nextPageWriteId - 1)),
                        rec, (state->numReads - reads));

                    if (i % 100 == 0) {
                        l = i / 100 - 1;
                        printf("Num: %lu KEY: %lu Records: %d Reads: %d\n", i,
                               mv, rec, (state->numReads - reads));
                        if (l < NUM_STEPS && l >= 0) {
                            rtimes[l][r] =
                                ((clock() - start) * 1000) / CLOCKS_PER_SEC;
                            rreads[l][r] = state->numReads;
                            rhits[l][r] = state->bufferHits;
                        }
                    }
                }
            }
        }

        end = clock();
        l = NUM_STEPS - 1;
        rtimes[l][r] = ((double)(end - start) * 1000) / CLOCKS_PER_SEC;
        uint32_t clocks = CLOCKS_PER_SEC;
        rreads[l][r] = state->numReads;
        rhits[l][r] = state->bufferHits;
        printf("Elapsed Time: %lu ms\n", rtimes[l][r]);
        printf("Records queried: %lu\n", i);

        printStats(state);

        // Optional: Test iterator
        // testIterator(state);
        // printStats(state);

        free(recordBuffer);
        fclose(state->file);
        free(state->buffer);
        free(state);
    }
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. */
void updateBitmapInt8Bucket(void *data, void *bm) {
    // Note: Assuming int key is right at the start of the data record
    int32_t val = *((int16_t *)data);
    uint8_t *bmval = (uint8_t *)bm;

    if (val < 10)
        *bmval = *bmval | 128;
    else if (val < 20)
        *bmval = *bmval | 64;
    else if (val < 30)
        *bmval = *bmval | 32;
    else if (val < 40)
        *bmval = *bmval | 16;
    else if (val < 50)
        *bmval = *bmval | 8;
    else if (val < 60)
        *bmval = *bmval | 4;
    else if (val < 100)
        *bmval = *bmval | 2;
    else
        *bmval = *bmval | 1;
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. Build bitmap based on min and
 * max value. */
void buildBitmapInt8BucketWithRange(void *min, void *max, void *bm) {
    /* Note: Assuming int key is right at the start of the data record */
    uint8_t *bmval = (uint8_t *)bm;

    if (min == NULL && max == NULL) {
        *bmval = 255; /* Everything */
    } else {
        int8_t i = 0;
        uint8_t val = 128;
        if (min != NULL) {
            /* Set bits based on min value */
            updateBitmapInt8Bucket(min, bm);

            /* Assume here that bits are set in increasing order based on
             * smallest value */
            /* Find first set bit */
            while ((val & *bmval) == 0 && i < 8) {
                i++;
                val = val / 2;
            }
            val = val / 2;
            i++;
        }
        if (max != NULL) {
            /* Set bits based on min value */
            updateBitmapInt8Bucket(max, bm);

            while ((val & *bmval) == 0 && i < 8) {
                i++;
                *bmval = *bmval + val;
                val = val / 2;
            }
        } else {
            while (i < 8) {
                i++;
                *bmval = *bmval + val;
                val = val / 2;
            }
        }
    }
}

int8_t inBitmapInt8Bucket(void *data, void *bm) {
    uint8_t *bmval = (uint8_t *)bm;

    uint8_t tmpbm = 0;
    updateBitmapInt8Bucket(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/* A 16-bit bitmap on a 32-bit int value */
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
    if (result < 0) return -1;
    if (result > 0) return 1;
    return 0;
}

uint32_t randomData(void **data, uint32_t sizeLowerBound,
                    uint32_t sizeUpperBound) {
    uint32_t size = rand() % (sizeUpperBound - sizeLowerBound) + sizeLowerBound;
    *data = malloc(size);
    for (uint32_t i = 0; i < size; i++) {
        *((uint8_t *)data + i) = rand() % UINT8_MAX;
    }
    return size;
}
