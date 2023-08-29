#include "schema.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * @brief	Create an embedDBSchema from a list of column sizes including both key and data
 * @param	numCols			The total number of columns in table
 * @param	colSizes		An array with the size of each column. Max size is 127
 * @param	colSignedness	An array describing if the data in the column is signed or unsigned. Use the defined constants embedDB_COLUMNN_SIGNED or embedDB_COLUMN_UNSIGNED
 */
embedDBSchema* embedDBCreateSchema(uint8_t numCols, int8_t* colSizes, int8_t* colSignedness) {
    embedDBSchema* schema = malloc(sizeof(embedDBSchema));
    schema->columnSizes = malloc(numCols * sizeof(int8_t));
    schema->numCols = numCols;
    uint16_t totalSize = 0;
    for (uint8_t i = 0; i < numCols; i++) {
        int8_t sign = colSignedness[i];
        uint8_t colSize = colSizes[i];
        totalSize += colSize;
        if (colSize <= 0) {
#ifdef PRINT_ERRORS
            printf("ERROR: Column size must be greater than zero\n");
#endif
            return NULL;
        }
        if (sign == embedDB_COLUMN_SIGNED) {
            schema->columnSizes[i] = -colSizes[i];
        } else if (sign == embedDB_COLUMN_UNSIGNED) {
            schema->columnSizes[i] = colSizes[i];
        } else {
#ifdef PRINT_ERRORS
            printf("ERROR: Must only use embedDB_COLUMN_SIGNED or embedDB_COLUMN_UNSIGNED to describe column signedness\n");
#endif
            return NULL;
        }
    }

    return schema;
}

/**
 * @brief	Free a schema. Sets the schema pointer to NULL.
 */
void embedDBFreeSchema(embedDBSchema** schema) {
    if (*schema == NULL) return;
    free((*schema)->columnSizes);
    free(*schema);
    *schema = NULL;
}

/**
 * @brief	Uses schema to determine the length of buffer to allocate and callocs that space
 */
void* createBufferFromSchema(embedDBSchema* schema) {
    uint16_t totalSize = 0;
    for (uint8_t i = 0; i < schema->numCols; i++) {
        totalSize += abs(schema->columnSizes[i]);
    }
    return calloc(1, totalSize);
}

/**
 * @brief	Deep copy schema and return a pointer to the copy
 */
embedDBSchema* copySchema(const embedDBSchema* schema) {
    embedDBSchema* copy = malloc(sizeof(embedDBSchema));
    if (copy == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while copying schema\n");
#endif
        return NULL;
    }
    copy->numCols = schema->numCols;
    copy->columnSizes = malloc(schema->numCols * sizeof(int8_t));
    if (copy->columnSizes == NULL) {
#ifdef PRINT_ERRORS
        printf("ERROR: malloc failed while copying schema\n");
#endif
        return NULL;
    }
    memcpy(copy->columnSizes, schema->columnSizes, schema->numCols * sizeof(int8_t));
    return copy;
}

/**
 * @brief	Finds byte offset of the column from the beginning of the record
 */
uint16_t getColOffsetFromSchema(embedDBSchema* schema, uint8_t colNum) {
    uint16_t pos = 0;
    for (uint8_t i = 0; i < colNum; i++) {
        pos += abs(schema->columnSizes[i]);
    }
    return pos;
}

/**
 * @brief	Calculates record size from schema
 */
uint16_t getRecordSizeFromSchema(embedDBSchema* schema) {
    uint16_t size = 0;
    for (uint8_t i = 0; i < schema->numCols; i++) {
        size += abs(schema->columnSizes[i]);
    }
    return size;
}

void printSchema(embedDBSchema* schema) {
    for (uint8_t i = 0; i < schema->numCols; i++) {
        if (i) {
            printf(", ");
        }
        int8_t col = schema->columnSizes[i];
        printf("%sint%d", embedDB_IS_COL_SIGNED(col) ? "" : "u", abs(col));
    }
    printf("\n");
}
