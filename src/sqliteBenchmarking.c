#include <errno.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
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
#define DATA_INDEX 1

#define RUN_TRANSACTION 1

int main() {
    int numRuns = 5;

    clock_t timeInsertNTText[numRuns],
        timeInsertNTBlob[numRuns],
        timeInsertNTInt[numRuns],
        timeInsertTText[numRuns],
        timeInsertTBlob[numRuns],
        timeInsertTInt[numRuns];
    uint32_t numRecordsInsertNTText,
        numRecordsInsertNTBlob,
        numRecordsInsertNTInt,
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
        FILE *dataset = fopen("data/uwa500K.bin", "rb");
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

        dropDatabase();
        db = NULL;
        query = NULL;

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
    printf("\nINSERT WITHOUT TRANSACTION INT TEXT\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeInsertNTText[i]);
        sum += timeInsertNTText[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records inserted: %d\n", numRecordsInsertNTText);

    sum = 0;
    printf("\nINSERT WITHOUT TRANSACTION INT BLOB\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeInsertNTBlob[i]);
        sum += timeInsertNTBlob[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records inserted: %d\n", numRecordsInsertNTBlob);

    sum = 0;
    printf("\nINSERT WITHOUT TRANSACTION INT INT INT INT\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeInsertNTInt[i]);
        sum += timeInsertNTInt[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records inserted: %d\n", numRecordsInsertNTInt);

    sum = 0;
    printf("\nINSERT WITH TRANSACTION INT TEXT\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeInsertTText[i]);
        sum += timeInsertTText[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records inserted: %d\n", numRecordsInsertTText);

    sum = 0;
    printf("\nINSERT WITH TRANSACTION INT BLOB\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeInsertTBlob[i]);
        sum += timeInsertTBlob[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records inserted: %d\n", numRecordsInsertTBlob);

    sum = 0;
    printf("\nINSERT WITH TRANSACTION INT INT INT INT\n");
    printf("Time: ");
    for (int i = 0; i < numRuns; i++) {
        printf("%d ", timeInsertTInt[i]);
        sum += timeInsertTInt[i];
    }
    printf("~ %dms\n", sum / numRuns);
    printf("Num Records inserted: %d\n", numRecordsInsertTInt);

    return 0;
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
    remove("build/artifacts/sqliteData.db");
    remove("build/artifacts/sqliteData.db-journal");
}

int setupDatabase() {
    /* Connect to Database */
    int openResult = sqlite3_open("build/artifacts/sqliteData.db", &db);
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
