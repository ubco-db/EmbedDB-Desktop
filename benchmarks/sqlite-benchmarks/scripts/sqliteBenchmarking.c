#include <errno.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

int createTableIntText();
int createTableIntBlob();
int createTableIntIntIntInt();
int setupDatabase();
void dropDatabase();
int getNumRecords();
int createIndexValue();
int createIndexAirTemp();
void printQueryExplain();

sqlite3 *db = NULL;

/*
 * 0: Journal
 * 1: WAL
 * 2: MEOMRY
 */
#define LOG_TYPE 1

/*
 * 0: Default
 * 1: Normal
 * 2: off
 */
#define SYNC 2

/*
 * 0: No Index
 * 1: With Index
 */
#define DATA_INDEX 0

#define RUN_TRANSACTION 1

char const databaseName[] = "sqliteData.db";
char const databaseJournalName[] = "sqliteData.db-journal";
char const dataFileName[] = "../../../data/uwa500K.bin";
char const randomizedDataFileName[] = "../../../data/uwa500K_randomized.bin";
int const numRuns = 1;

int main() {
    clock_t timeInsertNTText[numRuns],
        timeSelectStarText[numRuns],
        timeSelectStarSmallResultText[numRuns],
        timeSelectStarLargeResultText[numRuns],
        timeSelectDataSmallResultText[numRuns],
        timeSequentialKeyValueText[numRuns],
        timeRandomKeyValueText[numRuns],
        timeInsertNTBlob[numRuns],
        timeSelectStarBlob[numRuns],
        timeSelectStarSmallResultBlob[numRuns],
        timeSelectStarLargeResultBlob[numRuns],
        timeSelectDataSmallResultBlob[numRuns],
        timeSequentialKeyValueBlob[numRuns],
        timeRandomKeyValueBlob[numRuns],
        timeInsertNTInt[numRuns],
        timeSelectStarInt[numRuns],
        timeSelectStarSmallResultInt[numRuns],
        timeSelectStarLargeResultInt[numRuns],
        timeSelectDataSingleResultInt[numRuns],
        timeSelectDataSmallResultInt[numRuns],
        timeSelectDataLargeResultInt[numRuns],
        timeSelectKeyDataInt[numRuns],
        timeSequentialKeyValueInt[numRuns],
        timeRandomKeyValueInt[numRuns],
        timeInsertTText[numRuns],
        timeInsertTBlob[numRuns],
        timeInsertTInt[numRuns];
    uint32_t numRecordsInsertNTText,
        numRecordsSelectStarText,
        numRecordsSelectStarSmallResultText,
        numRecordsSelectStarLargeResultText,
        numRecordsSelectDataSmallResultText,
        numRecordsSequentialKeyValueText,
        numRecordsRandomKeyValueText,
        numRecordsInsertNTBlob,
        numRecordsSelectStarBlob,
        numRecordsSelectStarSmallResultBlob,
        numRecordsSelectStarLargeResultBlob,
        numRecordsSelectDataSmallResultBlob,
        numRecordsSequentialKeyValueBlob,
        numRecordsRandomKeyValueBlob,
        numRecordsInsertNTInt,
        numRecordsSelectStarInt,
        numRecordsSelectStarSmallResultInt,
        numRecordsSelectStarLargeResultInt,
        numRecordsSelectDataSingleResult,
        numRecordsSelectDataSmallResultInt,
        numRecordsSelectDataLargeResultInt,
        numRecordsSelectKeyDataInt,
        numRecordsSequentialKeyValueInt,
        numRecordsRandomKeyValueInt,
        numRecordsInsertTText,
        numRecordsInsertTBlob,
        numRecordsInsertTInt;

    for (int run = 0; run < numRuns; run++) {
        ///////////
        // Setup //
        ///////////

        setupDatabase();
        createTableIntText();

        sqlite3_stmt *query = NULL;
        int8_t recordSize = 16;
        int8_t dataSize = 12;
        int8_t keySize = 4;

        ////////////////////////////////////////////////
        // Insert uwa dataset no transaction int text //
        ////////////////////////////////////////////////
        char const insert[] = "INSERT INTO keyValue VALUES (?, ?);";
        FILE *dataset = fopen(dataFileName, "rb");
        char dataPage[512];

        timeInsertNTText[run] = clock();

        sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
        sqlite3_prepare_v2(db, insert, 36, &query, NULL);
        while (fread(dataPage, 512, 1, dataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                sqlite3_bind_int(query, 1, *(uint32_t *)(dataPage + record * recordSize));
                sqlite3_bind_text(query, 2, (char *)(dataPage + record * recordSize + keySize), dataSize, SQLITE_STATIC);
                sqlite3_step(query);
                sqlite3_reset(query);
            }
        }
        sqlite3_finalize(query);
        sqlite3_exec(db, "END TRANSACTION", NULL, NULL, NULL);

        timeInsertNTText[run] = (clock() - timeInsertNTText[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsInsertNTText = getNumRecords();
        query = NULL;

        ////////////////////////////
        // SELECT * FROM keyValue //
        ////////////////////////////
        int numRecords = 0;
        char const selectStar[] = "SELECT * FROM keyValue;";

        timeSelectStarText[run] = clock();

        sqlite3_prepare_v2(db, selectStar, strlen(selectStar), &query, NULL);
        while (sqlite3_step(query) == SQLITE_ROW) {
            numRecords++;
        }
        sqlite3_finalize(query);

        timeSelectStarText[run] = (clock() - timeSelectStarText[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSelectStarText = numRecords;

        query = NULL;

        ///////////////////////////////////////////////////
        // SELECT * FROM keyValue WHERE key >= 974100996 //
        ///////////////////////////////////////////////////
        numRecords = 0;
        char const selectStarSmallResult[] = "SELECT * FROM keyValue WHERE key >= 974100996";

        timeSelectStarSmallResultText[run] = clock();

        sqlite3_prepare_v2(db, selectStarSmallResult, strlen(selectStarSmallResult), &query, NULL);
        while (sqlite3_step(query) == SQLITE_ROW) {
            numRecords++;
        }
        sqlite3_finalize(query);

        timeSelectStarSmallResultText[run] = (clock() - timeSelectStarSmallResultText[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSelectStarSmallResultText = numRecords;

        query = NULL;

        ///////////////////////////////////////////////////
        // SELECT * FROM keyValue WHERE key >= 949756644 //
        ///////////////////////////////////////////////////

        numRecords = 0;
        char const selectStarLargeResult[] = "SELECT * FROM keyValue WHERE key >= 949756644";

        timeSelectStarLargeResultText[run] = clock();

        sqlite3_prepare_v2(db, selectStarLargeResult, strlen(selectStarLargeResult), &query, NULL);
        while (sqlite3_step(query) == SQLITE_ROW) {
            numRecords++;
        }
        sqlite3_finalize(query);

        timeSelectStarLargeResultText[run] = (clock() - timeSelectStarLargeResultText[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSelectStarLargeResultText = numRecords;

        query = NULL;

        //////////////////////////////////////
        // Sequential Key-Value Lookup Text //
        //////////////////////////////////////

        numRecords = 0;
        char const sequentialKeyValue[] = "SELECT * FROM keyValue WHERE key = ?";
        fseek(dataset, 0, SEEK_SET);
        timeSequentialKeyValueText[run] = clock();

        sqlite3_prepare_v2(db, sequentialKeyValue, strlen(sequentialKeyValue), &query, NULL);
        while (fread(dataPage, 512, 1, dataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                sqlite3_bind_int(query, 1, *(uint32_t *)(dataPage + record * recordSize));
                sqlite3_step(query);
                sqlite3_reset(query);
                numRecords++;
            }
        }
        sqlite3_finalize(query);

        timeSequentialKeyValueText[run] = (clock() - timeSequentialKeyValueText[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSequentialKeyValueText = numRecords;

        query = NULL;

        //////////////////////////////////
        // Random Key-Value Lookup Text //
        //////////////////////////////////

        numRecords = 0;
        char const randomKeyValue[] = "SELECT * FROM keyValue WHERE key = ?";
        FILE *randomDataset = fopen(randomizedDataFileName, "rb");
        timeRandomKeyValueText[run] = clock();

        sqlite3_prepare_v2(db, randomKeyValue, strlen(randomKeyValue), &query, NULL);
        while (fread(dataPage, 512, 1, randomDataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                sqlite3_bind_int(query, 1, *(uint32_t *)(dataPage + record * recordSize));
                sqlite3_step(query);
                sqlite3_reset(query);
                numRecords++;
            }
        }
        sqlite3_finalize(query);

        timeRandomKeyValueText[run] = (clock() - timeRandomKeyValueText[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsRandomKeyValueText = numRecords;

        query = NULL;

        //////////////////////////////////////////////
        // SELECT * FROM keyValue WHERE data >= 700 //
        //////////////////////////////////////////////

        /*
         * This was an attempt to extract the first four bytes of the text value field, which represents a
         * number in the test data set. Due to how casting works, we were unable to find a quick way to
         * extract this value. A recursive query or custom function would have to be developed to extract
         * the data this way.
         */

        /* This is not currently working */

        // numRecords = 0;
        // char const selectDataSmallResultText[] = "SELECT * FROM keyValue;";
        // timeSelectDataSmallResultText[run] = clock();

        // sqlite3_prepare_v2(db, selectDataSmallResultText, strlen(selectDataSmallResultText), &query, NULL);
        // while (sqlite3_step(query) == SQLITE_ROW) {
        //     numRecords++;
        // }
        // sqlite3_finalize(query);

        // timeSelectDataSmallResultText[run] = (clock() - timeSelectDataSmallResultText[run]) / (CLOCKS_PER_SEC / 1000);
        // numRecordsSelectDataSmallResultText = numRecords;

        query = NULL;

        dropDatabase();
        db = NULL;

        ////////////////////////////////////////////////
        // Insert uwa dataset no transaction int blob //
        ////////////////////////////////////////////////
        setupDatabase();
        createTableIntBlob();
        fseek(dataset, 0, SEEK_SET);

        timeInsertNTBlob[run] = clock();

        sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
        sqlite3_prepare_v2(db, insert, 36, &query, NULL);
        while (fread(dataPage, 512, 1, dataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                sqlite3_bind_int(query, 1, *(uint32_t *)(dataPage + record * recordSize));
                sqlite3_bind_blob(query, 2, (void *)(dataPage + record * recordSize + keySize), dataSize, NULL);
                sqlite3_step(query);
                sqlite3_reset(query);
            }
        }
        sqlite3_finalize(query);
        sqlite3_exec(db, "END TRANSACTION", NULL, NULL, NULL);

        timeInsertNTBlob[run] = (clock() - timeInsertNTBlob[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsInsertNTBlob = getNumRecords();

        query = NULL;

        ////////////////////////////
        // SELECT * FROM keyValue //
        ////////////////////////////
        numRecords = 0;

        timeSelectStarBlob[run] = clock();

        sqlite3_prepare_v2(db, selectStar, strlen(selectStar), &query, NULL);
        while (sqlite3_step(query) == SQLITE_ROW) {
            numRecords++;
        }
        sqlite3_finalize(query);

        timeSelectStarBlob[run] = (clock() - timeSelectStarBlob[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSelectStarBlob = numRecords;

        query = NULL;

        ///////////////////////////////////////////////////
        // SELECT * FROM keyValue WHERE key >= 974100996 //
        ///////////////////////////////////////////////////
        numRecords = 0;

        timeSelectStarSmallResultBlob[run] = clock();

        sqlite3_prepare_v2(db, selectStarSmallResult, strlen(selectStarSmallResult), &query, NULL);
        while (sqlite3_step(query) == SQLITE_ROW) {
            numRecords++;
        }
        sqlite3_finalize(query);

        timeSelectStarSmallResultBlob[run] = (clock() - timeSelectStarSmallResultBlob[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSelectStarSmallResultBlob = numRecords;

        query = NULL;

        ///////////////////////////////////////////////////
        // SELECT * FROM keyValue WHERE key >= 949756644 //
        ///////////////////////////////////////////////////

        numRecords = 0;

        timeSelectStarLargeResultBlob[run] = clock();

        sqlite3_prepare_v2(db, selectStarLargeResult, strlen(selectStarLargeResult), &query, NULL);
        while (sqlite3_step(query) == SQLITE_ROW) {
            numRecords++;
        }
        sqlite3_finalize(query);

        timeSelectStarLargeResultBlob[run] = (clock() - timeSelectStarLargeResultBlob[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSelectStarLargeResultBlob = numRecords;

        query = NULL;

        //////////////////////////////////////
        // Sequential Key-Value Lookup Text //
        //////////////////////////////////////

        numRecords = 0;
        fseek(dataset, 0, SEEK_SET);
        timeSequentialKeyValueBlob[run] = clock();

        sqlite3_prepare_v2(db, sequentialKeyValue, strlen(sequentialKeyValue), &query, NULL);
        while (fread(dataPage, 512, 1, dataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                sqlite3_bind_int(query, 1, *(uint32_t *)(dataPage + record * recordSize));
                sqlite3_step(query);
                sqlite3_reset(query);
                numRecords++;
            }
        }
        sqlite3_finalize(query);

        timeSequentialKeyValueBlob[run] = (clock() - timeSequentialKeyValueBlob[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSequentialKeyValueBlob = numRecords;

        query = NULL;

        //////////////////////////////////
        // Random Key-Value Lookup Text //
        //////////////////////////////////

        numRecords = 0;
        fseek(randomDataset, 0, SEEK_SET);
        timeRandomKeyValueBlob[run] = clock();

        sqlite3_prepare_v2(db, randomKeyValue, strlen(randomKeyValue), &query, NULL);
        while (fread(dataPage, 512, 1, randomDataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                sqlite3_bind_int(query, 1, *(uint32_t *)(dataPage + record * recordSize));
                sqlite3_step(query);
                sqlite3_reset(query);
                numRecords++;
            }
        }
        sqlite3_finalize(query);

        timeRandomKeyValueBlob[run] = (clock() - timeRandomKeyValueBlob[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsRandomKeyValueBlob = numRecords;

        query = NULL;

        //////////////////////////////////////////////
        // SELECT * FROM keyValue WHERE data >= 700 //
        //////////////////////////////////////////////

        /*
         * This was an attempt to extract the first four bytes of the blob value field, which represents a
         * number in the test data set. Due to how casting works, we were unable to find a quick way to
         * extract this value. A recursive query or custom function would have to be developed to extract
         * the data this way.
         */

        // numRecords = 0;
        // char const selectDataSmallResultBlob[] = "SELECT * FROM keyValue WHERE CAST(substr(value, 1, 4) AS INTEGER) = CAST(CAST(450 AS TEXT) AS INTEGER);";
        // char const selectDataSmallResultBlob[] = "SELECT * FROM keyValue WHERE CAST(hex(substr(value, 1, 4)) AS INTEGER) >= X'BC020000';";
        // timeSelectDataSmallResultBlob[run] = clock();
        // int32_t value = 700;

        // sqlite3_prepare_v2(db, selectDataSmallResultBlob, strlen(selectDataSmallResultBlob), &query, NULL);
        // sqlite3_bind_blob(query, 1, &value, 4, SQLITE_STATIC);
        // while (sqlite3_step(query) == SQLITE_ROW) {
        // void *blobResult = sqlite3_column_blob(query, 1);
        // int32_t test = 0;
        // memcpy(&test, blobResult, 4);
        // if (test < 700) {
        //     printf("Hello\n");
        // }
        //     numRecords++;
        // }
        // sqlite3_finalize(query);

        // timeSelectDataSmallResultBlob[run] = (clock() - timeSelectDataSmallResultBlob[run]) / (CLOCKS_PER_SEC / 1000);
        // numRecordsSelectDataSmallResultBlob = numRecords;

        // query = NULL;

        dropDatabase();
        db = NULL;
        query = NULL;

        ///////////////////////////////////////////////////////
        // Insert uwa dataset no transaction int int int int //
        ///////////////////////////////////////////////////////
        char const insertInts[] = "INSERT INTO keyValue VALUES (?, ?, ?, ?);";
        setupDatabase();
        createTableIntIntIntInt();
        fseek(dataset, 0, SEEK_SET);

        timeInsertNTInt[run] = clock();

        sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
        sqlite3_prepare_v2(db, insertInts, 42, &query, NULL);
        while (fread(dataPage, 512, 1, dataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                sqlite3_bind_int(query, 1, *(uint32_t *)(dataPage + record * recordSize));
                sqlite3_bind_int(query, 2, *(int32_t *)(dataPage + record * recordSize + keySize));
                sqlite3_bind_int(query, 3, *(int32_t *)(dataPage + record * recordSize + keySize + 4));
                sqlite3_bind_int(query, 4, *(int32_t *)(dataPage + record * recordSize + keySize + 8));
                sqlite3_step(query);
                sqlite3_reset(query);
            }
        }
        sqlite3_finalize(query);
        sqlite3_exec(db, "END TRANSACTION", NULL, NULL, NULL);

        timeInsertNTInt[run] = (clock() - timeInsertNTInt[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsInsertNTInt = getNumRecords();

        query = NULL;

        printQueryExplain();

        ////////////////////////////
        // SELECT * FROM keyValue //
        ////////////////////////////
        numRecords = 0;

        timeSelectStarInt[run] = clock();

        sqlite3_prepare_v2(db, selectStar, strlen(selectStar), &query, NULL);
        while (sqlite3_step(query) == SQLITE_ROW) {
            numRecords++;
        }
        sqlite3_finalize(query);

        timeSelectStarInt[run] = (clock() - timeSelectStarInt[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSelectStarInt = numRecords;

        query = NULL;

        ///////////////////////////////////////////////////
        // SELECT * FROM keyValue WHERE key >= 974100996 //
        ///////////////////////////////////////////////////
        numRecords = 0;

        timeSelectStarSmallResultInt[run] = clock();

        sqlite3_prepare_v2(db, selectStarSmallResult, strlen(selectStarSmallResult), &query, NULL);
        while (sqlite3_step(query) == SQLITE_ROW) {
            numRecords++;
        }
        sqlite3_finalize(query);

        timeSelectStarSmallResultInt[run] = (clock() - timeSelectStarSmallResultInt[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSelectStarSmallResultInt = numRecords;

        query = NULL;

        ///////////////////////////////////////////////////
        // SELECT * FROM keyValue WHERE key >= 949756644 //
        ///////////////////////////////////////////////////

        numRecords = 0;

        timeSelectStarLargeResultInt[run] = clock();

        sqlite3_prepare_v2(db, selectStarLargeResult, strlen(selectStarLargeResult), &query, NULL);
        while (sqlite3_step(query) == SQLITE_ROW) {
            numRecords++;
        }
        sqlite3_finalize(query);

        timeSelectStarLargeResultInt[run] = (clock() - timeSelectStarLargeResultInt[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSelectStarLargeResultInt = numRecords;

        query = NULL;

        /////////////////////////////////////////////////
        // SELECT * FROM keyValue WHERE airTemp = 800 //
        /////////////////////////////////////////////////

        numRecords = 0;
        char const selectDataSingleResult[] = "SELECT * FROM keyValue WHERE airTemp = 800;";

        timeSelectDataSingleResultInt[run] = clock();

        sqlite3_prepare_v2(db, selectDataSingleResult, strlen(selectDataSingleResult), &query, NULL);
        while (sqlite3_step(query) == SQLITE_ROW) {
            numRecords++;
        }
        sqlite3_finalize(query);

        timeSelectDataSingleResultInt[run] = (clock() - timeSelectDataSingleResultInt[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSelectDataSingleResult = numRecords;

        query = NULL;

        /////////////////////////////////////////////////
        // SELECT * FROM keyValue WHERE airTemp >= 700 //
        /////////////////////////////////////////////////

        numRecords = 0;
        char const selectDataSmallResultInt[] = "SELECT * FROM keyValue WHERE airTemp >= 700;";

        timeSelectDataSmallResultInt[run] = clock();

        sqlite3_prepare_v2(db, selectDataSmallResultInt, strlen(selectDataSmallResultInt), &query, NULL);
        while (sqlite3_step(query) == SQLITE_ROW) {
            numRecords++;
        }
        sqlite3_finalize(query);

        timeSelectDataSmallResultInt[run] = (clock() - timeSelectDataSmallResultInt[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSelectDataSmallResultInt = numRecords;

        query = NULL;

        ///////////////////////////////////////
        // SELECT * FROM r WHERE data >= 420 //
        ///////////////////////////////////////

        numRecords = 0;
        char const selectDataLargeResultInt[] = "SELECT * FROM keyValue WHERE airTemp >= 420;";

        timeSelectDataLargeResultInt[run] = clock();

        sqlite3_prepare_v2(db, selectDataLargeResultInt, strlen(selectDataLargeResultInt), &query, NULL);
        while (sqlite3_step(query) == SQLITE_ROW) {
            numRecords++;
        }
        sqlite3_finalize(query);

        timeSelectDataLargeResultInt[run] = (clock() - timeSelectDataLargeResultInt[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSelectDataLargeResultInt = numRecords;

        query = NULL;

        ////////////////////////////////////////////////////////////////////////////
        // SELECT * FROM r WHERE key >= 958885776 AND data >= 450 AND data <= 650 //
        ////////////////////////////////////////////////////////////////////////////

        numRecords = 0;
        char const selectKeyDataResultInt[] = "SELECT * FROM keyValue WHERE key >= 958885776 AND airTemp >= 450 AND airTemp <= 650;";

        timeSelectKeyDataInt[run] = clock();

        sqlite3_prepare_v2(db, selectKeyDataResultInt, strlen(selectKeyDataResultInt), &query, NULL);
        while (sqlite3_step(query) == SQLITE_ROW) {
            numRecords++;
        }
        sqlite3_finalize(query);

        timeSelectKeyDataInt[run] = (clock() - timeSelectKeyDataInt[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSelectKeyDataInt = numRecords;

        query = NULL;

        /////////////////////////////////////////
        // Sequential Key-Value Lookup Integer //
        /////////////////////////////////////////

        numRecords = 0;
        fseek(dataset, 0, SEEK_SET);
        timeSequentialKeyValueInt[run] = clock();

        sqlite3_prepare_v2(db, sequentialKeyValue, strlen(sequentialKeyValue), &query, NULL);
        while (fread(dataPage, 512, 1, dataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                sqlite3_bind_int(query, 1, *(uint32_t *)(dataPage + record * recordSize));
                sqlite3_step(query);
                sqlite3_reset(query);
                numRecords++;
            }
        }
        sqlite3_finalize(query);

        timeSequentialKeyValueInt[run] = (clock() - timeSequentialKeyValueInt[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSequentialKeyValueInt = numRecords;

        query = NULL;

        /////////////////////////////////////
        // Random Key-Value Lookup Integer //
        /////////////////////////////////////

        numRecords = 0;
        fseek(randomDataset, 0, SEEK_SET);
        timeRandomKeyValueInt[run] = clock();

        sqlite3_prepare_v2(db, randomKeyValue, strlen(randomKeyValue), &query, NULL);
        while (fread(dataPage, 512, 1, randomDataset)) {
            uint16_t count = *(uint16_t *)(dataPage + 4);
            for (int record = 1; record <= count; record++) {
                sqlite3_bind_int(query, 1, *(uint32_t *)(dataPage + record * recordSize));
                sqlite3_step(query);
                sqlite3_reset(query);
                numRecords++;
            }
        }
        sqlite3_finalize(query);

        timeRandomKeyValueInt[run] = (clock() - timeRandomKeyValueInt[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsRandomKeyValueInt = numRecords;

        query = NULL;

        dropDatabase();
        db = NULL;
        query = NULL;

        if (RUN_TRANSACTION == 1) {
            //////////////////////////////////////////////////
            // Insert uwa dataset with transaction int text //
            //////////////////////////////////////////////////
            setupDatabase();
            createTableIntText();
            fseek(dataset, 0, SEEK_SET);

            timeInsertTText[run] = clock();

            sqlite3_prepare_v2(db, insert, 36, &query, NULL);
            while (fread(dataPage, 512, 1, dataset)) {
                uint16_t count = *(uint16_t *)(dataPage + 4);
                for (int record = 1; record <= count; record++) {
                    sqlite3_bind_int(query, 1, *(uint32_t *)(dataPage + record * recordSize));
                    sqlite3_bind_text(query, 2, (char *)(dataPage + record * recordSize + keySize), dataSize, SQLITE_STATIC);
                    sqlite3_step(query);
                    sqlite3_reset(query);
                }
            }
            sqlite3_finalize(query);

            timeInsertTText[run] = (clock() - timeInsertTText[run]) / (CLOCKS_PER_SEC / 1000);
            numRecordsInsertTText = getNumRecords();

            dropDatabase();
            db = NULL;
            query = NULL;

            //////////////////////////////////////////////////
            // Insert uwa dataset with transaction int blob //
            //////////////////////////////////////////////////
            setupDatabase();
            createTableIntBlob();
            fseek(dataset, 0, SEEK_SET);

            timeInsertTBlob[run] = clock();

            sqlite3_prepare_v2(db, insert, 36, &query, NULL);
            while (fread(dataPage, 512, 1, dataset)) {
                uint16_t count = *(uint16_t *)(dataPage + 4);
                for (int record = 1; record <= count; record++) {
                    sqlite3_bind_int(query, 1, *(uint32_t *)(dataPage + record * recordSize));
                    sqlite3_bind_blob(query, 2, (void *)(dataPage + record * recordSize + keySize), dataSize, NULL);
                    sqlite3_step(query);
                    sqlite3_reset(query);
                }
            }
            sqlite3_finalize(query);

            timeInsertTBlob[run] = (clock() - timeInsertTBlob[run]) / (CLOCKS_PER_SEC / 1000);
            numRecordsInsertTBlob = getNumRecords();

            dropDatabase();
            db = NULL;
            query = NULL;

            /////////////////////////////////////////////////////////
            // Insert uwa dataset with transaction int int int int //
            /////////////////////////////////////////////////////////
            setupDatabase();
            createTableIntIntIntInt();
            fseek(dataset, 0, SEEK_SET);

            timeInsertTInt[run] = clock();

            sqlite3_prepare_v2(db, insertInts, 42, &query, NULL);
            while (fread(dataPage, 512, 1, dataset)) {
                uint16_t count = *(uint16_t *)(dataPage + 4);
                for (int record = 1; record <= count; record++) {
                    sqlite3_bind_int(query, 1, *(uint32_t *)(dataPage + record * recordSize));
                    sqlite3_bind_int(query, 2, *(int32_t *)(dataPage + record * recordSize + keySize));
                    sqlite3_bind_int(query, 3, *(int32_t *)(dataPage + record * recordSize + keySize + 4));
                    sqlite3_bind_int(query, 4, *(int32_t *)(dataPage + record * recordSize + keySize + 8));
                    sqlite3_step(query);
                    sqlite3_reset(query);
                }
            }
            sqlite3_finalize(query);

            timeInsertTInt[run] = (clock() - timeInsertTInt[run]) / (CLOCKS_PER_SEC / 1000);
            numRecordsInsertTInt = getNumRecords();

            dropDatabase();
            db = NULL;
            query = NULL;
        }
    }

    ///////////////////////////
    // Print out the results //
    ///////////////////////////
    int sum = 0;
    printf("\nINSERT WITHOUT TRANSACTION INTEGER TEXT\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeInsertNTText[i]);
        sum += timeInsertNTText[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records inserted: %d\n", numRecordsInsertNTText);

    sum = 0;
    printf("\nINSERT WITHOUT TRANSACTION INTEGER BLOB\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeInsertNTBlob[i]);
        sum += timeInsertNTBlob[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records inserted: %d\n", numRecordsInsertNTBlob);

    sum = 0;
    printf("\nINSERT WITHOUT TRANSACTION INTEGER INTEGER INTEGER INTEGER\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeInsertNTInt[i]);
        sum += timeInsertNTInt[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records inserted: %d\n", numRecordsInsertNTInt);

    sum = 0;
    printf("\nINSERT WITH TRANSACTION INTEGER TEXT\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeInsertTText[i]);
        sum += timeInsertTText[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records inserted: %d\n", numRecordsInsertTText);

    sum = 0;
    printf("\nINSERT WITH TRANSACTION INTEGER BLOB\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeInsertTBlob[i]);
        sum += timeInsertTBlob[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records inserted: %d\n", numRecordsInsertTBlob);

    sum = 0;
    printf("\nINSERT WITH TRANSACTION INTEGER INTEGER INTEGER INTEGER\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeInsertTInt[i]);
        sum += timeInsertTInt[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records inserted: %d\n", numRecordsInsertTInt);

    sum = 0;
    printf("\nSELECT * FROM KEYVALUE INTEGER TEXT\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectStarText[i]);
        sum += timeSelectStarText[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSelectStarText);

    sum = 0;
    printf("\nSELECT * FROM KEYVALUE SMALL RESULT INTEGER TEXT\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectStarSmallResultText[i]);
        sum += timeSelectStarSmallResultText[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSelectStarSmallResultText);

    sum = 0;
    printf("\nSELECT * FROM KEYVALUE LARGE RESULT INTEGER TEXT\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectStarLargeResultText[i]);
        sum += timeSelectStarLargeResultText[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSelectStarLargeResultText);

    sum = 0;
    printf("\nSEQUENTIAL KEY VALUE LOOKUP INTEGER TEXT\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSequentialKeyValueText[i]);
        sum += timeSequentialKeyValueText[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSequentialKeyValueText);

    sum = 0;
    printf("\nRANDOM KEY VALUE LOOKUP INTEGER TEXT\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeRandomKeyValueText[i]);
        sum += timeRandomKeyValueText[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsRandomKeyValueText);

    // sum = 0;
    // printf("\nSELECT * FROM KEYVALUE DATA SMALL RESULT INTEGER TEXT\n");
    // printf("Time: ");
    // for (int i = 0; i < numRuns; i++) {
    //     printf("%d ", timeSelectDataSmallResultText[i]);
    //     sum += timeSelectDataSmallResultText[i];
    // }
    // printf("~ %dms\n", sum / numRuns);
    // printf("Num Records Queried: %d\n", numRecordsSelectDataSmallResultText);

    sum = 0;
    printf("\nSELECT * FROM KEYVALUE INTEGER BLOB\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectStarBlob[i]);
        sum += timeSelectStarBlob[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSelectStarBlob);

    sum = 0;
    printf("\nSELECT * FROM KEYVALUE SMALL RESULT INTEGER BLOB\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectStarSmallResultBlob[i]);
        sum += timeSelectStarSmallResultBlob[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSelectStarSmallResultBlob);

    sum = 0;
    printf("\nSELECT * FROM KEYVALUE LARGE RESULT INTEGER BLOB\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectStarLargeResultBlob[i]);
        sum += timeSelectStarLargeResultBlob[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSelectStarLargeResultBlob);

    sum = 0;
    printf("\nSEQUENTIAL KEY VALUE LOOKUP INTEGER BLOB\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSequentialKeyValueBlob[i]);
        sum += timeSequentialKeyValueBlob[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSequentialKeyValueBlob);

    sum = 0;
    printf("\nRANDOM KEY VALUE LOOKUP INTEGER BLOB\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeRandomKeyValueBlob[i]);
        sum += timeRandomKeyValueBlob[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsRandomKeyValueBlob);

    /* This would print the data querying results from the blob format data, but the query did not return the correct number of records. */

    // sum = 0;
    // printf("\nSELECT * FROM KEYVALUE DATA SMALL RESULT INTEGER BLOB\n");
    // printf("Time: ");
    // for (int i = 0; i < numRuns; i++) {
    //     printf("%d ", timeSelectDataSmallResultBlob[i]);
    //     sum += timeSelectDataSmallResultBlob[i];
    // }
    // printf("~ %dms\n", sum / numRuns);
    // printf("Num Records Queried: %d\n", numRecordsSelectDataSmallResultBlob);

    sum = 0;
    printf("\nSELECT * FROM KEYVALUE INTEGER INTEGER INTEGER INTEGER\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectStarInt[i]);
        sum += timeSelectStarInt[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSelectStarInt);

    sum = 0;
    printf("\nSELECT * FROM KEYVALUE SMALL RESULT INTEGER INTEGER INTEGER INTEGER\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectStarSmallResultInt[i]);
        sum += timeSelectStarSmallResultInt[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSelectStarSmallResultInt);

    sum = 0;
    printf("\nSELECT * FROM KEYVALUE LARGE RESULT INTEGER INTEGER INTEGER INTEGER\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectStarLargeResultInt[i]);
        sum += timeSelectStarLargeResultInt[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSelectStarLargeResultInt);

    sum = 0;
    printf("\nSELECT * FROM KEYVALUE DATA SINGLE RESULT INTEGER INTEGER INTEGER INTEGER\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectDataSingleResultInt[i]);
        sum += timeSelectDataSingleResultInt[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSelectDataSingleResult);

    sum = 0;
    printf("\nSELECT * FROM KEYVALUE DATA SMALL RESULT INTEGER INTEGER INTEGER INTEGER\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectDataSmallResultInt[i]);
        sum += timeSelectDataSmallResultInt[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSelectDataSmallResultInt);

    sum = 0;
    printf("\nSELECT * FROM KEYVALUE DATA LARGE RESULT INTEGER INTEGER INTEGER INTEGER\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectDataLargeResultInt[i]);
        sum += timeSelectDataLargeResultInt[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSelectDataLargeResultInt);

    sum = 0;
    printf("\nSELECT * FROM KEYVALUE KEY DATA RESULT INTEGER INTEGER INTEGER INTEGER\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSelectKeyDataInt[i]);
        sum += timeSelectKeyDataInt[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSelectKeyDataInt);

    sum = 0;
    printf("\nSEQUENTIAL KEY VALUE LOOKUP INTEGER INTEGER INTEGER INTEGER\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeSequentialKeyValueInt[i]);
        sum += timeSequentialKeyValueInt[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSequentialKeyValueInt);

    sum = 0;
    printf("\nRANDOM KEY VALUE LOOKUP INTEGER INTEGER INTEGER INTEGER\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeRandomKeyValueInt[i]);
        sum += timeRandomKeyValueInt[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsRandomKeyValueInt);

    return 0;
}

void printQueryExplain() {
    sqlite3_stmt *query = NULL;
    char dataQueryLarge[] = "EXPLAIN QUERY PLAN SELECT * FROM keyValue WHERE airTemp >= 420;";
    sqlite3_prepare_v2(db, dataQueryLarge, strlen(dataQueryLarge), &query, NULL);
    sqlite3_step(query);
    printf("Large Data Query Plan: %s\n", sqlite3_column_text(query, 3));
    sqlite3_finalize(query);
    char dataQuerySmall[] = "EXPLAIN QUERY PLAN SELECT * FROM keyValue WHERE airTemp >= 700;";
    sqlite3_prepare_v2(db, dataQuerySmall, strlen(dataQuerySmall), &query, NULL);
    sqlite3_step(query);
    printf("Small Data Query Plan: %s\n", sqlite3_column_text(query, 3));
    sqlite3_finalize(query);
    char dataQuerySingleSelect[] = "EXPLAIN QUERY PLAN SELECT * FROM keyValue WHERE airTemp = 800;";
    sqlite3_prepare_v2(db, dataQuerySingleSelect, strlen(dataQuerySingleSelect), &query, NULL);
    sqlite3_step(query);
    printf("Single Data Value Query Plan: %s\n",  sqlite3_column_text(query, 3));
    sqlite3_finalize(query);
}

int createTableIntText() {
    /* Setup Table */
    sqlite3_stmt *ps = NULL;
    char const createStatement[] = "CREATE TABLE keyValue (key INTEGER PRIMARY KEY, value TEXT);";
    int prepareResult = sqlite3_prepare_v2(db, createStatement, 60, &ps, NULL);
    if (prepareResult != 0) {
        printf("Error preparing the statement!\n");
        return -1;
    }

    int createResult = sqlite3_step(ps);
    if (createResult != SQLITE_DONE) {
        printf("There was a problem creating the table!");
        return 1;
    }
    sqlite3_finalize(ps);

    if (DATA_INDEX == 1) {
        createIndexValue();
    }
}

int createTableIntBlob() {
    /* Setup Table */
    sqlite3_stmt *ps = NULL;
    char const createStatement[] = "CREATE TABLE keyValue (key INTEGER PRIMARY KEY, value BLOB);";
    int prepareResult = sqlite3_prepare_v2(db, createStatement, 60, &ps, NULL);
    if (prepareResult != 0) {
        printf("Error preparing the statement!\n");
        return -1;
    }

    int createResult = sqlite3_step(ps);
    if (createResult != SQLITE_DONE) {
        printf("There was a problem creating the table!");
        return 1;
    }

    sqlite3_finalize(ps);

    if (DATA_INDEX == 1) {
        createIndexValue();
    }
}

int createTableIntIntIntInt() {
    /* Setup Table */
    sqlite3_stmt *ps = NULL;
    char const createStatement[] = "CREATE TABLE keyValue (key INTEGER PRIMARY KEY, airTemp INTEGER, airPressure INTEGER, windSpeed INTEGER);";
    int prepareResult = sqlite3_prepare_v2(db, createStatement, 106, &ps, NULL);
    if (prepareResult != 0) {
        printf("Error preparing the statement!\n");
        return -1;
    }

    int createResult = sqlite3_step(ps);
    if (createResult != SQLITE_DONE) {
        printf("There was a problem creating the table!");
        return 1;
    }

    sqlite3_finalize(ps);

    if (DATA_INDEX == 1) {
        createIndexAirTemp();
    }
}

void dropDatabase() {
    sqlite3_close(db);
    remove(databaseName);
    remove(databaseJournalName);
}

int setupDatabase() {
    /* Connect to Database */
    int openResult = sqlite3_open(databaseName, &db);
    if (openResult != 0) {
        printf("Error opening the database!\n");
        return -1;
    }
    if (LOG_TYPE == 1) {
        sqlite3_exec(db, "PRAGMA journal_mode = WAL;", NULL, NULL, NULL);
    }
    if (LOG_TYPE == 2) {
        sqlite3_exec(db, "PRAGMA journal_mode = MEMORY", NULL, NULL, NULL);
    }
    if (SYNC == 1) {
        sqlite3_exec(db, "PRAGMA synchronous = NORMAL;", NULL, NULL, NULL);
    }
    if (SYNC == 2) {
        sqlite3_exec(db, "PRAGMA synchronous = OFF;", NULL, NULL, NULL);
    }
    return 0;
}

int getNumRecords() {
    sqlite3_stmt *numRecords = NULL;
    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM keyValue;", -1, &numRecords, NULL);
    int result = sqlite3_step(numRecords);
    int num = sqlite3_column_int(numRecords, 0);
    sqlite3_step(numRecords);
    sqlite3_finalize(numRecords);
    return num;
}

int createIndexValue() {
    sqlite3_stmt *index = NULL;
    char const createIndex[] = "CREATE INDEX data ON keyValue (value);";
    int prepareResult = sqlite3_prepare_v2(db, createIndex, strlen(createIndex), &index, NULL);
    if (prepareResult != 0) {
        printf("Error preparing the statement!\n");
        return -1;
    }

    int createResult = sqlite3_step(index);
    if (createResult != SQLITE_DONE) {
        printf("There was a problem creating the index!");
        return 1;
    }

    sqlite3_finalize(index);
}

int createIndexAirTemp() {
    sqlite3_stmt *index = NULL;
    char const createIndex[] = "CREATE INDEX data ON keyValue (airTemp);";
    int prepareResult = sqlite3_prepare_v2(db, createIndex, strlen(createIndex), &index, NULL);
    if (prepareResult != 0) {
        printf("Error preparing the statement!\n");
        return -1;
    }

    int createResult = sqlite3_step(index);
    if (createResult != SQLITE_DONE) {
        printf("There was a problem creating the index!");
        return 1;
    }

    sqlite3_finalize(index);
}
