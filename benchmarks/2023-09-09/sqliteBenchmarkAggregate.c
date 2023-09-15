#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "sqlite3.h"

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
char const dataFileName[] = "../../data/uwa500K_only_100K.bin";
char const randomizedDataFileName[] = "../../data/uwa500K_only_100K_randomized.bin";
#define numRuns 3

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
        timeAggregate[numRuns],
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
        sqlite3_stmt *query = NULL;
        int8_t recordSize = 16;
        int8_t dataSize = 12;
        int8_t keySize = 4;
        char const insert[] = "INSERT INTO keyValue VALUES (?, ?);";
        FILE *dataset = fopen(dataFileName, "rb");
        char dataPage[512];

        ////////////
        // Insert //
        ////////////
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

        ////////////////////////////
        // SELECT * FROM keyValue //
        ////////////////////////////
        int numRecords = 0;

        timeAggregate[run] = clock();

        char const selectStar[] = "SELECT cast((key / 86400) as int), min(airTemp), max(airTemp), avg(airTemp) FROM keyValue GROUP BY cast((key / 86400) as int);";
        sqlite3_prepare_v2(db, selectStar, strlen(selectStar), &query, NULL);
        // Print out the results
        while (sqlite3_step(query) == SQLITE_ROW) {
            // printf("%d | %.2f | %.2f | %.2f\n", sqlite3_column_int(query, 0), sqlite3_column_double(query, 1) / 10.0, sqlite3_column_double(query, 2) / 10.0, sqlite3_column_double(query, 3) / 10.0);
            numRecords++;
        }
        sqlite3_finalize(query);

        timeAggregate[run] = (clock() - timeAggregate[run]) / (CLOCKS_PER_SEC / 1000);
        numRecordsSelectStarInt = numRecords;

        query = NULL;

        dropDatabase();
        db = NULL;
        query = NULL;
    }

    ///////////////////////////
    // Print out the results //
    ///////////////////////////
    int sum = 0;
    printf("\nAggregate\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeAggregate[i]);
        sum += timeAggregate[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records Queried: %d\n", numRecordsSelectStarInt);

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
    printf("Single Data Value Query Plan: %s\n", sqlite3_column_text(query, 3));
    sqlite3_finalize(query);
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