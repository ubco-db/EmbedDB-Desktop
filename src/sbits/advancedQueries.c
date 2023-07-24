#include "advancedQueries.h"

#include <string.h>

/**
 * @brief	Execute a function on each return of an iterator
 * @param	operator		An operator struct containing the input operator and function to run on the data
 * @param	schema			Pre-allocated space for the schema
 * @param	recordBuffer	Pre-allocated space for the whole record (key & data)
 * @return	1 if another (key, data) pair was returned, 0 if there are no more pairs to return
 */
int8_t exec(sbitsOperator* operator, sbitsSchema * schema, void* recordBuffer) {
    if (operator->input == NULL) {
        // Bottom level operator such as a table scan i.e. sbitsIterator
        return operator->func(schema, operator->info, recordBuffer);
    } else {
        // Execute `func` on top of the output of `input`
        while (exec(operator->input, schema, recordBuffer)) {
            if (operator->func(schema, operator->info, recordBuffer)) {
                return 1;
            }
        }
        return 0;
    }
}

/**
 * @brief	Calculate an aggregate function over specified groups
 * @param	input			An operator struct to pull input data from
 * @param	groupfunc		A function that returns whether or not the `key` is part of the same group as the `lastkey`. Assumes that groups are always next to each other when read in.
 * @param	operators		An array of operators, each of which will be updated with each record read from the iterator
 * @param	numOps			The number of sbitsAggrOps in `operators`
 * @param	schema			Pre-allocated space for the schema
 * @param	recordBuffer	Pre-allocated space for the operator to put the key. **NOT A RETURN VALUE**
 * @param	bufferSize		The length (in bytes) of `recordBuffer`
 * @return	1 if another group was calculated, 0 if not.
 */
int8_t aggroup(sbitsOperator* input, int8_t (*groupfunc)(void* lastRecord, void* record), sbitsAggrOp* operators, uint32_t numOps, sbitsSchema* schema, void* recordBuffer, uint8_t bufferSize) {
    // Reset each operator
    for (int i = 0; i < numOps; i++) {
        operators[i].reset(operators[i].state);
    }

    // Check if this call is the first for this query
    int8_t isFirstCall = 1;
    for (uint8_t i = 0; i < bufferSize; i++) {
        if (((uint8_t*)recordBuffer)[i] != 0xff) {
            isFirstCall = 0;
            break;
        }
    }

    void* lastRecord = malloc(bufferSize);

    // If it is not the first call, need to add() the last key
    if (!isFirstCall) {
        memcpy(lastRecord, recordBuffer, bufferSize);
        for (int i = 0; i < numOps; i++) {
            operators[i].add(operators[i].state, recordBuffer);
        }
    }

    // Get next record
    int8_t gotRecords = 0;
    while (exec(input, schema, recordBuffer)) {
        gotRecords = 1;
        // Check if record is in the same group as the last record
        if (isFirstCall || groupfunc(lastRecord, recordBuffer)) {
            isFirstCall = 0;
            for (int i = 0; i < numOps; i++) {
                operators[i].add(operators[i].state, recordBuffer);
            }
        } else {
            break;
        }

        // Save this record
        memcpy(lastRecord, recordBuffer, bufferSize);
    }

    // If true, then the iterator did not return any records on this call of aggroup()
    if (!gotRecords) {
        return 0;
    }

    // Perform final compute on all operators
    for (int i = 0; i < numOps; i++) {
        operators[i].compute(operators[i].state);
    }

    return 1;
}

uint16_t getColPos(sbitsSchema* schema, uint8_t colNum) {
    uint16_t pos = 0;
    for (uint8_t i = 0; i < colNum; i++) {
        pos += abs(schema->columnSizes[i]);
    }
    return pos;
}

int compareUnsignedNumbers(const void* num1, const void* num2, int8_t numBytes) {
    // Cast the pointers to unsigned char pointers for byte-wise comparison
    const uint8_t* bytes1 = (const uint8_t*)num1;
    const uint8_t* bytes2 = (const uint8_t*)num2;

    for (int8_t i = numBytes - 1; i >= 0; i--) {
        if (bytes1[i] < bytes2[i]) {
            return -1;
        } else if (bytes1[i] > bytes2[i]) {
            return 1;
        }
    }

    // Both numbers are equal
    return 0;
}

int compareSignedNumbers(const void* num1, const void* num2, int8_t numBytes) {
    // Cast the pointers to unsigned char pointers for byte-wise comparison
    const uint8_t* bytes1 = (const uint8_t*)num1;
    const uint8_t* bytes2 = (const uint8_t*)num2;

    // Check the sign bits of the most significant bytes
    int sign1 = bytes1[numBytes - 1] & 0x80;
    int sign2 = bytes2[numBytes - 1] & 0x80;

    if (sign1 != sign2) {
        // Different signs, negative number is smaller
        return (sign1 ? -1 : 1);
    }

    // Same sign, perform regular byte-wise comparison
    for (int8_t i = numBytes - 1; i >= 0; i--) {
        if (bytes1[i] < bytes2[i]) {
            return -1;
        } else if (bytes1[i] > bytes2[i]) {
            return 1;
        }
    }

    // Both numbers are equal
    return 0;
}

int8_t tableScan(sbitsSchema* schema, void* info, void* recordBuffer) {
    sbitsState* state = (sbitsState*)(((void**)info)[0]);
    sbitsIterator* it = (sbitsIterator*)(((void**)info)[1]);
    sbitsSchema* baseSchema = (sbitsSchema*)(((void**)info)[2]);
    if (!sbitsNext(state, it, recordBuffer, (int8_t*)recordBuffer + state->keySize)) {
        return 0;
    }
    // Copy base schema
    if (schema == NULL) {
        printf("ERROR: Must allocate space for the schema\n");
        return 0;
    }
    schema->numCols = baseSchema->numCols;
    schema->columnSizes = realloc(schema->columnSizes, baseSchema->numCols * sizeof(int8_t));
    if (schema->columnSizes == NULL) {
        printf("ERROR: Failed to realloc schema columnSizes will doing table scan");
        return 0;
    }
    memcpy(schema->columnSizes, baseSchema->columnSizes, baseSchema->numCols * sizeof(int8_t));
    return 1;
}

void* createTableScanInfo(sbitsState* state, sbitsIterator* it, sbitsSchema* baseSchema) {
    void* info = malloc(3 * sizeof(void*));
    memcpy(info, &state, sizeof(void*));
    memcpy((int8_t*)info + sizeof(void*), &it, sizeof(void*));
    memcpy((int8_t*)info + 2 * sizeof(void*), &baseSchema, sizeof(void*));
    return info;
}

int8_t projectionFunc(sbitsSchema* schema, void* info, void* recordBuffer) {
    uint8_t numCols = *(uint8_t*)info;
    uint8_t* cols = (uint8_t*)info + 1;
    uint16_t curColPos = 0;
    uint8_t nextProjCol = 0;
    uint16_t nextProjColPos = 0;
    for (uint8_t col = 0; col < schema->numCols && nextProjCol != numCols; col++) {
        int32_t ogColSize = schema->columnSizes[col];
        uint8_t colSize = abs(ogColSize);
        if (col == cols[nextProjCol]) {
            memcpy((int8_t*)recordBuffer + nextProjColPos, (int8_t*)recordBuffer + curColPos, colSize);
            schema->columnSizes[nextProjCol] = ogColSize;
            nextProjColPos += colSize;
            nextProjCol++;
        }
        curColPos += colSize;
    }
    // Shrink schema colsize array accordingly
    schema->numCols = numCols;
    schema->columnSizes = realloc(schema->columnSizes, numCols * sizeof(int8_t));
    if (schema->columnSizes == NULL) {
        printf("ERROR: Failed to realloc schema columnSizes will doing table scan");
        return 0;
    }
    return 1;
}

void* createProjectionInfo(uint8_t numCols, uint8_t* cols) {
    // Ensure column numbers are strictly increasing
    uint8_t lastCol = cols[0];
    for (uint8_t i = 1; i < numCols; i++) {
        if (cols[i] <= lastCol) {
            printf("ERROR: Columns in a projection must be strictly ascending for performance reasons");
            return NULL;
        }
        lastCol = cols[i];
    }
    // Create info
    uint8_t* info = malloc(numCols + 1);
    info[0] = numCols;
    memcpy(info + 1, cols, numCols);
    return info;
}

int8_t selectionFunc(sbitsSchema* schema, void* info, void* recordBuffer) {
    int8_t colNum = *(int8_t*)info;
    int8_t operation = *((int8_t*)info + 1);
    int8_t colSize = schema->columnSizes[colNum];
    int8_t isSigned = 0;
    if (colSize < 0) {
        colSize = -colSize;
        isSigned = 1;
    }

    void* colData = (int8_t*)recordBuffer + getColPos(schema, colNum);
    if (isSigned) {
        switch (operation) {
            case SELECT_GT:
                return compareSignedNumbers(colData, *(void**)((int8_t*)info + 2), colSize) > 0;
            case SELECT_LT:
                return compareSignedNumbers(colData, *(void**)((int8_t*)info + 2), colSize) < 0;
            case SELECT_GTE:
                return compareSignedNumbers(colData, *(void**)((int8_t*)info + 2), colSize) >= 0;
            case SELECT_LTE:
                return compareSignedNumbers(colData, *(void**)((int8_t*)info + 2), colSize) <= 0;
            default:
                return 0;
        }
    } else {
        switch (operation) {
            case SELECT_GT:
                return compareUnsignedNumbers(colData, *(void**)((int8_t*)info + 2), colSize) > 0;
            case SELECT_LT:
                return compareUnsignedNumbers(colData, *(void**)((int8_t*)info + 2), colSize) < 0;
            case SELECT_GTE:
                return compareUnsignedNumbers(colData, *(void**)((int8_t*)info + 2), colSize) >= 0;
            case SELECT_LTE:
                return compareUnsignedNumbers(colData, *(void**)((int8_t*)info + 2), colSize) <= 0;
            default:
                return 0;
        }
    }
}

void* createSelectinfo(int8_t colNum, int8_t operation, void* compVal) {
    int8_t* data = malloc(2 + sizeof(void*));
    data[0] = colNum;
    data[1] = operation;
    memcpy(data + 2, &compVal, sizeof(void*));
    return data;
}

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
    free((*schema)->columnSizes);
    free(*schema);
    *schema = NULL;
}
