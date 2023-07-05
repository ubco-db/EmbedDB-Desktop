#include <errno.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

int createTableIntText();
int setupDatabase();
void dropDatabase();
void printNumRecords();

sqlite3 *db = NULL;

int main() {
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
    printf("\nINSERT WITH NO TRANSACTION\n");
    char const insert[] = "INSERT INTO keyValue VALUES (?, ?);";
    int numRecords = 0;
    FILE *dataset = fopen("data/uwa500K.bin", "rb");
    char dataPage[512];

    clock_t start = clock();
    sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
    sqlite3_prepare_v2(db, insert, 36, &query, NULL);
    while (fread(dataPage, 512, 1, dataset)) {
        uint16_t count = *(uint16_t *)(dataPage + 4);
        for (int record = 1; record <= count; record++) {
            uint32_t key = *(uint32_t *)(dataPage + record * recordSize);
            sqlite3_bind_int(query, 1, key);
            sqlite3_bind_text(query, 2, (char *)(dataPage + record * recordSize + keySize), dataSize, SQLITE_STATIC);
            sqlite3_step(query);
            sqlite3_reset(query);
            numRecords++;
        }
    }
    sqlite3_finalize(query);
    sqlite3_exec(db, "END TRANSACTION", NULL, NULL, NULL);
    clock_t timeInsert = (clock() - start) / (CLOCKS_PER_SEC / 1000);

    printNumRecords();
    printf("Num Records inserted: %d\n", numRecords);
    printf("Time: %dms\n", timeInsert);

    dropDatabase();
    db = NULL;
    query = NULL;

    //////////////////////////////////////////////////
    // Insert uwa dataset with transaction int text //
    //////////////////////////////////////////////////
    setupDatabase();
    createTableIntText();
    printf("\nINSERT WITH TRANSACTION\n");
    numRecords = 0;
    fseek(dataset, 0, SEEK_SET);

    start = clock();
    sqlite3_prepare_v2(db, insert, 36, &query, NULL);
    while (fread(dataPage, 512, 1, dataset)) {
        uint16_t count = *(uint16_t *)(dataPage + 4);
        for (int record = 1; record <= count; record++) {
            uint32_t key = *(uint32_t *)(dataPage + record * recordSize);
            sqlite3_bind_int(query, 1, key);
            sqlite3_bind_text(query, 2, (char *)(dataPage + record * recordSize + keySize), dataSize, SQLITE_STATIC);
            sqlite3_step(query);
            sqlite3_reset(query);
            numRecords++;
        }
    }
    sqlite3_finalize(query);
    timeInsert = (clock() - start) / (CLOCKS_PER_SEC / 1000);

    printNumRecords();
    printf("Num Records inserted: %d\n", numRecords);
    printf("Time: %dms\n", timeInsert);

    dropDatabase();
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
}

void dropDatabase() {
    sqlite3_close(db);
    remove("build/artifacts/sqliteData.db");
}

int setupDatabase() {
    /* Connect to Database */
    int openResult = sqlite3_open("build/artifacts/sqliteData.db", &db);
    if (openResult != 0) {
        printf("Error opening the database!\n");
        return -1;
    }
}

void printNumRecords() {
    sqlite3_stmt *numRecords = NULL;
    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM keyValue;", -1, &numRecords, NULL);
    int result = sqlite3_step(numRecords);
    int num = sqlite3_column_int(numRecords, 0);
    printf("Number of records: %i\n", num);
    sqlite3_step(numRecords);
    sqlite3_finalize(numRecords);
}
