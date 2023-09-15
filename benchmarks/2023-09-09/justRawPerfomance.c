#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

int main() {
    printf("Starting RAW performance test.\n");

    char buffer[512];
    clock_t start;
    /* SD Card */
    FILE *fp = fopen("speedTestTemp.bin", "w+b");
    if (fp == NULL) {
        printf("Error opening file.\n");
        return 0;
    }

    // Test time to write 100000 blocks
    int numWrites = 100000;
    start = clock();

    for (int i = 0; i < numWrites; i++) {
        for (int j = 0; j < 128; j++) {
            ((uint32_t *)buffer)[j]++;
        }
        if (0 == fwrite(buffer, 512, 1, fp)) {
            printf("Write error.\n");
        }
        if (i % 100 == 0) {
            fflush(fp);
        }
    }
    printf("Write time: %lums (%.2f MB/s)\n", (clock() - start) / (CLOCKS_PER_SEC / 1000), (double)numWrites * 512 / 1000000 / ((clock() - start) / (CLOCKS_PER_SEC / 1000)) * 1000);

    start = clock();

    for (int i = 0; i < numWrites; i++) {
        for (int j = 0; j < 128; j++) {
            ((uint32_t *)buffer)[j]++;
        }
        unsigned long num = rand() % numWrites;
        fseek(fp, num * 512, SEEK_SET);
        if (0 == fwrite(buffer, 512, 1, fp)) {
            printf("Write error.\n");
        }
        if (i % 100 == 0) {
            fflush(fp);
        }
    }
    printf("Random write time: %lums (%.2f MB/s)\n", (clock() - start) / (CLOCKS_PER_SEC / 1000), (double)numWrites * 512 / 1000000 / ((clock() - start) / (CLOCKS_PER_SEC / 1000)) * 1000);

    // Time to read 1000 blocks
    fseek(fp, 0, SEEK_SET);
    start = clock();
    for (int i = 0; i < numWrites; i++) {
        for (int j = 0; j < 128; j++) {
            ((uint32_t *)buffer)[j]++;
        }
        if (0 == fread(buffer, 512, 1, fp)) {
            printf("Read error.\n");
        }
    }
    printf("Read time: %lums (%.2f MB/s)\n", (clock() - start) / (CLOCKS_PER_SEC / 1000), (double)numWrites * 512 / 1000000 / ((clock() - start) / (CLOCKS_PER_SEC / 1000)) * 1000);

    fseek(fp, 0, SEEK_SET);
    // Time to read 1000 blocks randomly
    start = clock();
    srand(1);
    for (int i = 0; i < numWrites; i++) {
        for (int j = 0; j < 128; j++) {
            ((uint32_t *)buffer)[j]++;
        }
        unsigned long num = rand() % numWrites;
        fseek(fp, num * 512, SEEK_SET);
        if (0 == fread(buffer, 512, 1, fp)) {
            printf("Read error.\n");
        }
    }
    printf("Random Read time: %lums (%.2f MB/s)\n", (clock() - start) / (CLOCKS_PER_SEC / 1000), (double)numWrites * 512 / 1000000 / ((clock() - start) / (CLOCKS_PER_SEC / 1000)) * 1000);

    return 0;
}