#include "schema.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * @brief	Create an sbitsSchema from a list of column sizes including both key and data
 * @param	numCols			The total number of key & data columns in table
 * @param	colSizes		An array with the size of each column. Max size is 127
 * @param	colSignedness	An array describing if the data in the column is SBITS_COLUMNN_SIGNED or SBITS_COLUMN_UNSIGNED
 */
sbitsSchema* sbitsCreateSchema(uint8_t numCols, int8_t* colSizes, int8_t* colSignedness) {
    if (numCols < 2) {
        printf("ERROR: When creating a schema, you must include all key and data attributes\n");
        return NULL;
    }

    // Check that the provided key schema matches what is in the state
    if (colSignedness[0] != SBITS_COLUMN_UNSIGNED) {
        printf("ERROR: Make sure the the key column is at index 0 of the schema initialization and that it matches the keySize in the state and is unsigned\n");
        return NULL;
    }

    sbitsSchema* schema = malloc(sizeof(sbitsSchema));
    schema->columnSizes = malloc(numCols * sizeof(int8_t));
    schema->numCols = numCols;
    uint16_t totalSize = 0;
    for (uint8_t i = 0; i < numCols; i++) {
        int8_t sign = colSignedness[i];
        uint8_t colSize = colSizes[i];
        totalSize += colSize;
        if (colSize <= 0) {
            printf("ERROR: Column size must be greater than zero\n");
            return NULL;
        }
        if (sign == SBITS_COLUMN_SIGNED) {
            schema->columnSizes[i] = -colSizes[i];
        } else if (sign == SBITS_COLUMN_UNSIGNED) {
            schema->columnSizes[i] = colSizes[i];
        } else {
            printf("ERROR: Invalid signedness of column\n");
            return NULL;
        }
    }

    return schema;
}

/**
 * @brief	Free a schema. Sets the schema pointer to NULL.
 */
void sbitsFreeSchema(sbitsSchema** schema) {
    if (*schema == NULL) return;
    free((*schema)->columnSizes);
    free(*schema);
    *schema = NULL;
}

/**
 * @brief	Uses schema to determine the length of buffer to allocate and callocs that space
 */
void* createBufferFromSchema(sbitsSchema* schema) {
    uint16_t totalSize = 0;
    for (uint8_t i = 0; i < schema->numCols; i++) {
        totalSize += abs(schema->columnSizes[i]);
    }
    return calloc(1, totalSize);
}

/**
 * @brief	Deep copy schema and return a pointer to the copy
 */
sbitsSchema* copySchema(const sbitsSchema* schema) {
    sbitsSchema* copy = malloc(sizeof(sbitsSchema));
    if (copy == NULL) {
        printf("ERROR: malloc failed while copying schema\n");
        return NULL;
    }
    copy->numCols = schema->numCols;
    copy->columnSizes = malloc(schema->numCols * sizeof(int8_t));
    if (copy->columnSizes == NULL) {
        printf("ERROR: malloc failed while copying schema\n");
        return NULL;
    }
    memcpy(copy->columnSizes, schema->columnSizes, schema->numCols * sizeof(int8_t));
    return copy;
}

/**
 * @brief	Finds byte offset of the column from the beginning of the record
 */
uint16_t getColOffsetFromSchema(sbitsSchema* schema, uint8_t colNum) {
    uint16_t pos = 0;
    for (uint8_t i = 0; i < colNum; i++) {
        pos += abs(schema->columnSizes[i]);
    }
    return pos;
}

/**
 * @brief	Calculates record size from schema
 */
uint16_t getRecordSizeFromSchema(sbitsSchema* schema) {
    uint16_t pos = 0;
    for (uint8_t i = 0; i < schema->numCols; i++) {
        pos += abs(schema->columnSizes[i]);
    }
    return pos;
}

void printSchema(sbitsSchema* schema) {
    for (uint8_t i = 0; i < schema->numCols; i++) {
        if (i) {
            printf(", ");
        }
        int8_t col = schema->columnSizes[i];
        printf("%sint%d", SBITS_IS_COL_SIGNED(col) ? "" : "u", abs(col));
    }
    printf("\n");
}
