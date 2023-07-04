#include <errno.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

void createTable();

int main() {
    ///////////
    // Setup //
    ///////////

    /* Connect to Database */
    sqlite3 *db = NULL;
    int openResult = sqlite3_open("build/artifacts/sqliteData.db", &db);
    if (openResult != 0) {
        printf("Error opening the database!\n");
        return -1;
    }

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

    sqlite3_stmt *query = NULL;
    char const insert[] = "INSERT INTO keyValue VALUES (?, ?);";
    int8_t recordSize = 16;
    int8_t dataSize = 12;
    int8_t keySize = 4;

    ////////////////////////
    // Insert uwa dataset //
    ////////////////////////
    printf("\nINSERT\n");
    clock_t start = clock();

    int numRecords = 0;
    FILE *dataset = fopen("data/uwa500K.bin", "rb");
    char dataPage[512];

    sqlite3_prepare_v2(db, insert, 36, &query, NULL);

    while (fread(dataPage, 512, 1, dataset)) {
        uint16_t count = *(uint16_t *)(dataPage + 4);
        for (int record = 1; record <= count; record++) {
            sqlite3_bind_int(query, 1, *(uint32_t *)(dataPage + record * recordSize));
            sqlite3_bind_text(query, 2, (char *)(dataPage + record * recordSize + keySize), dataSize, SQLITE_STATIC);
            // sbitsPut(state, dataPage + record * state->recordSize, dataPage + record * state->recordSize + state->keySize);
            int insertResult = sqlite3_step(query);
            if (insertResult != SQLITE_DONE) {
                printf("Error with insert!\n");
                return 1;
            }

            sqlite3_reset(query);
            numRecords++;
            if (numRecords % 10000 == 0) {
                printf("Hello\n");
            }
        }
    }

    sqlite3_finalize(query);

    clock_t timeInsert = (clock() - start) / (CLOCKS_PER_SEC / 1000);

    printf("Num Records inserted: %d\n", numRecords);
    printf("Time: %dms\n", timeInsert);

    sqlite3_close(db);
}

void createTable() {

}