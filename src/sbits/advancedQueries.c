#include "advancedQueries.h"

#include <string.h>

/**
 * @brief	Execute a function on each return of an iterator
 * @param	operator		An operator struct containing the input operator and function to run on the data
 * @param	recordBuffer	Pre-allocated space for the whole record (key & data)
 * @return	1 if another (key, data) pair was returned, 0 if there are no more pairs to return
 */
int8_t exec(sbitsOperator* operator, void * recordBuffer) {
    if (operator->input == NULL) {
        // Bottom level operator such as a table scan i.e. sbitsIterator
        return operator->func(operator, recordBuffer);
    } else {
        // Execute `func` on top of the output of `input`
        while (exec(operator->input, recordBuffer)) {
            if (operator->func(operator, recordBuffer)) {
                return 1;
            }
        }
        return 0;
    }
}

/**
 * @brief	Calculate an aggregate function over specified groups
 * @param	operator		An operator struct to pull input data from
 * @param	recordBuffer	Pre-allocated space for the operator to put the key. **NOT A RETURN VALUE**
 * @return	1 if another group was calculated, 0 if not.
 */
int8_t aggregateFunc(sbitsOperator* operator, void * recordBuffer) {
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(sbitsSchema));
        operator->schema->numCols = 2;
        operator->schema->columnSizes = malloc(2 * sizeof(int8_t));
        int8_t colSizes[] = {4, 4};
        memcpy(operator->schema->columnSizes, colSizes, 2);
    }

    int8_t* info = operator->info;
    int8_t (*groupfunc)(const void* lastRecord, const void* record) = *(void**)info;
    sbitsAggrOp* operators = *(void**)(info + sizeof(void*));
    uint32_t numOps = *(uint32_t*)(info + 2 * sizeof(void*));
    void* lastRecordBuffer = *(void**)(info + 2 * sizeof(void*) + sizeof(uint32_t));
    uint8_t bufferSize = *(uint8_t*)(info + 3 * sizeof(void*) + sizeof(uint32_t));

    sbitsOperator* input = operator->input;

    // Reset each operator
    for (int i = 0; i < numOps; i++) {
        operators[i].reset(operators[i].state);
    }

    // Check if this call is the first for this query
    int8_t isFirstCall = 1;
    for (uint8_t i = 0; i < bufferSize; i++) {
        if (((uint8_t*)lastRecordBuffer)[i] != 0xff) {
            isFirstCall = 0;
            break;
        }
    }

    // If it is not the first call, need to add() the last key
    if (!isFirstCall) {
        for (int i = 0; i < numOps; i++) {
            operators[i].add(operators[i].state, lastRecordBuffer);
        }
    }

    // We already have a new record read into recordBuffer, so process it
    if (isFirstCall || groupfunc(lastRecordBuffer, recordBuffer)) {
        isFirstCall = 0;
        for (int i = 0; i < numOps; i++) {
            operators[i].add(operators[i].state, recordBuffer);
        }
        memcpy(lastRecordBuffer, recordBuffer, bufferSize);

        while (exec(input, recordBuffer)) {
            // Check if record is in the same group as the last record
            if (groupfunc(lastRecordBuffer, recordBuffer)) {
                for (int i = 0; i < numOps; i++) {
                    operators[i].add(operators[i].state, recordBuffer);
                }
            } else {
                break;
            }

            // Save this record
            memcpy(lastRecordBuffer, recordBuffer, bufferSize);
        }
    }

    // Perform final compute on all operators
    // Save copy of record that was just read in. It's not part of the group, but we can't lose it because we need it for the next call. Can't put it in lastRecordBuffer either because we need the contents of that for compute()
    void* temp = malloc(bufferSize);
    memcpy(temp, recordBuffer, bufferSize);
    for (int i = 0; i < numOps; i++) {
        operators[i].compute(operators[i].state, input->schema, recordBuffer, lastRecordBuffer);
    }
    memcpy(lastRecordBuffer, temp, bufferSize);
    free(temp);

    return 1;
}

/**
 * @brief	Create the info for an aggregate operator
 * @param	groupfunc		A function that returns whether or not the `key` is part of the same group as the `lastkey`. Assumes that groups are always next to each other when read in.
 * @param	operators		An array of operators, each of which will be updated with each record read from the iterator
 * @param	numOps			The number of sbitsAggrOps in `operators`
 * @param	lastRecordBuffer	A secondary buffer needed to store the last key that was read for the purpose of comparing it to the record that was just read. Needs to be the same size as `recordBuffer`
 * @param	bufferSize		The length (in bytes) of `recordBuffer`
 * @return	Returns a pointer to the info object to be put into a sbitsOperator
 */
void* createAggregateInfo(int8_t (*groupfunc)(const void* lastRecord, const void* record), sbitsAggrOp* operators, uint32_t numOps, void* lastRecordBuffer, uint8_t bufferSize) {
    int8_t* info = malloc(3 * sizeof(void*) + sizeof(uint32_t) + sizeof(uint8_t));
    memcpy(info, &groupfunc, sizeof(void*));
    memcpy(info + sizeof(void*), &operators, sizeof(void*));
    memcpy(info + 2 * sizeof(void*), &numOps, sizeof(uint32_t));
    memcpy(info + 2 * sizeof(void*) + sizeof(uint32_t), &lastRecordBuffer, sizeof(void*));
    memcpy(info + 3 * sizeof(void*) + sizeof(uint32_t), &bufferSize, sizeof(uint8_t));
    return info;
}

uint16_t getColPos(sbitsSchema* schema, uint8_t colNum) {
    uint16_t pos = 0;
    for (uint8_t i = 0; i < colNum; i++) {
        pos += abs(schema->columnSizes[i]);
    }
    return pos;
}

int8_t tableScan(sbitsOperator* operator, void * recordBuffer) {
    // Check that a schema was set
    if (operator->schema == NULL) {
        printf("ERROR: Must provide a base schema for a table scan operator\n");
        return 0;
    }

    // Get next record
    sbitsState* state = (sbitsState*)(((void**)operator->info)[0]);
    sbitsIterator* it = (sbitsIterator*)(((void**)operator->info)[1]);
    if (!sbitsNext(state, it, recordBuffer, (int8_t*)recordBuffer + state->keySize)) {
        return 0;
    }

    return 1;
}

void* createTableScanInfo(sbitsState* state, sbitsIterator* it) {
    void* info = malloc(2 * sizeof(void*));
    memcpy(info, &state, sizeof(void*));
    memcpy((int8_t*)info + sizeof(void*), &it, sizeof(void*));
    return info;
}

int8_t projectionFunc(sbitsOperator* operator, void * recordBuffer) {
    uint8_t numCols = *(uint8_t*)operator->info;
    uint8_t* cols = (uint8_t*)operator->info + 1;
    uint16_t curColPos = 0;
    uint8_t nextProjCol = 0;
    uint16_t nextProjColPos = 0;
    const sbitsSchema* inputSchema = operator->input->schema;

    if (operator->schema == NULL) {
        // Build output schema
        operator->schema = malloc(sizeof(sbitsSchema));
        operator->schema->numCols = numCols;
        operator->schema->columnSizes = malloc(numCols * sizeof(int8_t));
        for (uint8_t i = 0; i < numCols; i++) {
            operator->schema->columnSizes[i] = cols[i];
        }
    }

    for (uint8_t col = 0; col < inputSchema->numCols && nextProjCol != numCols; col++) {
        uint8_t colSize = abs(inputSchema->columnSizes[col]);
        if (col == cols[nextProjCol]) {
            memcpy((int8_t*)recordBuffer + nextProjColPos, (int8_t*)recordBuffer + curColPos, colSize);
            nextProjColPos += colSize;
            nextProjCol++;
        }
        curColPos += colSize;
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

int8_t selectionFunc(sbitsOperator* operator, void * recordBuffer) {
    sbitsSchema* inputSchema = operator->input->schema;

    if (operator->schema == NULL) {
        // Copy schema from input operator
        operator->schema = malloc(sizeof(sbitsSchema));
        operator->schema->numCols = inputSchema->numCols;
        operator->schema->columnSizes = malloc(inputSchema->numCols * sizeof(int8_t));
        memcpy(operator->schema->columnSizes, inputSchema->columnSizes, inputSchema->numCols);
    }

    int8_t colNum = *(int8_t*)operator->info;
    int8_t operation = *((int8_t*)operator->info + 1);
    int8_t colSize = inputSchema->columnSizes[colNum];
    int8_t isSigned = 0;
    if (colSize < 0) {
        colSize = -colSize;
        isSigned = 1;
    }

    void* colData = (int8_t*)recordBuffer + getColPos(inputSchema, colNum);
    if (isSigned) {
        switch (operation) {
            case SELECT_GT:
                return compareSignedNumbers(colData, *(void**)((int8_t*)operator->info + 2), colSize) > 0;
            case SELECT_LT:
                return compareSignedNumbers(colData, *(void**)((int8_t*)operator->info + 2), colSize) < 0;
            case SELECT_GTE:
                return compareSignedNumbers(colData, *(void**)((int8_t*)operator->info + 2), colSize) >= 0;
            case SELECT_LTE:
                return compareSignedNumbers(colData, *(void**)((int8_t*)operator->info + 2), colSize) <= 0;
            default:
                return 0;
        }
    } else {
        switch (operation) {
            case SELECT_GT:
                return compareUnsignedNumbers(colData, *(void**)((int8_t*)operator->info + 2), colSize) > 0;
            case SELECT_LT:
                return compareUnsignedNumbers(colData, *(void**)((int8_t*)operator->info + 2), colSize) < 0;
            case SELECT_GTE:
                return compareUnsignedNumbers(colData, *(void**)((int8_t*)operator->info + 2), colSize) >= 0;
            case SELECT_LTE:
                return compareUnsignedNumbers(colData, *(void**)((int8_t*)operator->info + 2), colSize) <= 0;
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
 * @brief	Completely free a chain of operators recursively. Does not recursively free any pointer contained in `sbitsOperator::info`
 */
void sbitsDestroyOperatorRecursive(sbitsOperator** operator) {
    if ((*operator)->input != NULL) {
        sbitsDestroyOperatorRecursive(&(*operator)->input);
    }
    if ((*operator)->info != NULL) {
        free((*operator)->info);
    }
    if ((*operator)->schema != NULL) {
        sbitsFreeSchema(&(*operator)->schema);
    }
    free(*operator);
    (*operator) = NULL;
}
