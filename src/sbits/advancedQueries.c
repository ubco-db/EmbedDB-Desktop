#include "advancedQueries.h"

#include <string.h>

int8_t compareUnsignedNumbers(const void* num1, const void* num2, int8_t numBytes) {
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

int8_t compareSignedNumbers(const void* num1, const void* num2, int8_t numBytes) {
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

int8_t compare(void* a, uint8_t operation, void* b, int8_t isSigned, int8_t numBytes) {
    int8_t (*compFunc)(const void* num1, const void* num2, int8_t numBytes) = isSigned ? compareSignedNumbers : compareUnsignedNumbers;
    switch (operation) {
        case SELECT_GT:
            return compFunc(a, b, numBytes) > 0;
        case SELECT_LT:
            return compFunc(a, b, numBytes) < 0;
        case SELECT_GTE:
            return compFunc(a, b, numBytes) >= 0;
        case SELECT_LTE:
            return compFunc(a, b, numBytes) <= 0;
        case SELECT_EQ:
            return compFunc(a, b, numBytes) == 0;
        case SELECT_NEQ:
            return compFunc(a, b, numBytes) != 0;
        default:
            return 0;
    }
}

/**
 * @brief	Execute a function on each return of an iterator
 * @param	operator		An operator struct containing the input operator and function to run on the data
 * @param	recordBuffer	Pre-allocated space for the whole record (key & data)
 * @return	1 if another (key, data) pair was returned, 0 if there are no more pairs to return
 */
int8_t exec(sbitsOperator* operator) {
    return operator->next(operator);
}

void initTableScan(sbitsOperator* operator) {
    if (operator->input != NULL) {
        printf("WARNING: TableScan operator should not have an input operator\n");
    }
    if (operator->schema == NULL) {
        printf("ERROR: TableScan operator needs its schema defined\n");
        return;
    }

    // Init buffer
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
            return;
        }
    }
}

int8_t nextTableScan(sbitsOperator* operator) {
    // Check that a schema was set
    if (operator->schema == NULL) {
        printf("ERROR: Must provide a base schema for a table scan operator\n");
        return 0;
    }

    // Get next record
    sbitsState* state = (sbitsState*)(((void**)operator->info)[0]);
    sbitsIterator* it = (sbitsIterator*)(((void**)operator->info)[1]);
    if (!sbitsNext(state, it, operator->recordBuffer, (int8_t*)operator->recordBuffer + state->keySize)) {
        return 0;
    }

    return 1;
}

void closeTableScan(sbitsOperator* operator) {
    sbitsFreeSchema(&operator->schema);
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
    free(operator->info);
    operator->info = NULL;
}

sbitsOperator* createTableScanOperator(sbitsState* state, sbitsIterator* it, sbitsSchema* baseSchema) {
    // Ensure all fields are not NULL
    if (state == NULL || it == NULL || baseSchema == NULL) {
        printf("ERROR: All fields must be provided to create a TableScan operator\n");
        return NULL;
    }

    sbitsOperator* operator= malloc(sizeof(sbitsOperator));
    if (operator== NULL) {
        printf("ERROR: malloc failed while creating TableScan operator\n");
        return NULL;
    }

    operator->info = malloc(2 * sizeof(void*));
    if (operator->info == NULL) {
        printf("ERROR: malloc failed while creating TableScan operator\n");
        return NULL;
    }
    memcpy(operator->info, &state, sizeof(void*));
    memcpy((int8_t*)operator->info + sizeof(void*), &it, sizeof(void*));

    operator->schema = copySchema(baseSchema);
    operator->input = NULL;
    operator->recordBuffer = NULL;

    operator->init = initTableScan;
    operator->next = nextTableScan;
    operator->close = closeTableScan;

    return operator;
}

void initProjection(sbitsOperator* operator) {
    if (operator->input == NULL) {
        printf("ERROR: Projection operator needs an input operator\n");
        return;
    }

    // Init input
    operator->input->init(operator->input);

    // Get info
    uint8_t numCols = *(uint8_t*)operator->info;
    uint8_t* cols = (uint8_t*)operator->info + 1;
    const sbitsSchema* inputSchema = operator->input->schema;

    // Init output schema
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(sbitsSchema));
        if (operator->schema == NULL) {
            printf("ERROR: Failed to allocate space for projection schema\n");
            return;
        }
        operator->schema->numCols = numCols;
        operator->schema->columnSizes = malloc(numCols * sizeof(int8_t));
        if (operator->schema->columnSizes == NULL) {
            printf("ERROR: Failed to allocate space for projection while building schema\n");
            return;
        }
        for (uint8_t i = 0; i < numCols; i++) {
            operator->schema->columnSizes[i] = inputSchema->columnSizes[cols[i]];
        }
    }

    // Init output buffer
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
            return;
        }
    }
}

int8_t nextProjection(sbitsOperator* operator) {
    uint8_t numCols = *(uint8_t*)operator->info;
    uint8_t* cols = (uint8_t*)operator->info + 1;
    uint16_t curColPos = 0;
    uint8_t nextProjCol = 0;
    uint16_t nextProjColPos = 0;
    const sbitsSchema* inputSchema = operator->input->schema;

    // Get next record
    if (operator->input->next(operator->input)) {
        for (uint8_t col = 0; col < inputSchema->numCols && nextProjCol != numCols; col++) {
            uint8_t colSize = abs(inputSchema->columnSizes[col]);
            if (col == cols[nextProjCol]) {
                memcpy((int8_t*)operator->recordBuffer + nextProjColPos, (int8_t*)operator->input->recordBuffer + curColPos, colSize);
                nextProjColPos += colSize;
                nextProjCol++;
            }
            curColPos += colSize;
        }
        return 1;
    } else {
        return 0;
    }
}

void closeProjection(sbitsOperator* operator) {
    operator->input->close(operator->input);

    sbitsFreeSchema(&operator->schema);
    free(operator->info);
    operator->info = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

sbitsOperator* createProjectionOperator(sbitsOperator* input, uint8_t numCols, uint8_t* cols) {
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
    if (info == NULL) {
        printf("ERROR: malloc failed while creating Projection operator\n");
        return NULL;
    }
    info[0] = numCols;
    memcpy(info + 1, cols, numCols);

    sbitsOperator* operator= malloc(sizeof(sbitsOperator));
    if (operator== NULL) {
        printf("ERROR: malloc failed while creating Projection operator\n");
        return NULL;
    }

    operator->info = info;
    operator->input = input;
    operator->schema = NULL;
    operator->recordBuffer = NULL;
    operator->init = initProjection;
    operator->next = nextProjection;
    operator->close = closeProjection;

    return operator;
}

void initSelection(sbitsOperator* operator) {
    if (operator->input == NULL) {
        printf("ERROR: Projection operator needs an input operator\n");
        return;
    }

    // Init input
    operator->input->init(operator->input);

    // Init output schema
    if (operator->schema == NULL) {
        operator->schema = copySchema(operator->input->schema);
    }

    // Init output buffer
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
            printf("ERROR: Failed to allocate buffer for TableScan operator\n");
            return;
        }
    }
}

int8_t nextSelection(sbitsOperator* operator) {
    sbitsSchema* schema = operator->input->schema;

    int8_t colNum = *(int8_t*)operator->info;
    uint16_t colPos = getColPosFromSchema(schema, colNum);
    int8_t operation = *((int8_t*)operator->info + 1);
    int8_t colSize = schema->columnSizes[colNum];
    int8_t isSigned = 0;
    if (colSize < 0) {
        colSize = -colSize;
        isSigned = 1;
    }

    while (operator->input->next(operator->input)) {
        void* colData = (int8_t*)operator->input->recordBuffer + colPos;

        if (compare(colData, operation, *(void**)((int8_t*)operator->info + 2), isSigned, colSize)) {
            memcpy(operator->recordBuffer, operator->input->recordBuffer, getRecordSizeFromSchema(operator->schema));
            return 1;
        }
    }

    return 0;
}

void closeSelection(sbitsOperator* operator) {
    operator->input->close(operator->input);

    sbitsFreeSchema(&operator->schema);
    free(operator->info);
    operator->info = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

sbitsOperator* createSelectionOperator(sbitsOperator* input, int8_t colNum, int8_t operation, void* compVal) {
    int8_t* info = malloc(2 + sizeof(void*));
    if (info == NULL) {
        printf("ERROR: Failed to malloc while creating Selection operator\n");
        return NULL;
    }
    info[0] = colNum;
    info[1] = operation;
    memcpy(info + 2, &compVal, sizeof(void*));

    sbitsOperator* operator= malloc(sizeof(sbitsOperator));
    if (operator== NULL) {
        printf("ERROR: Failed to malloc while creating Selection operator\n");
        return NULL;
    }
    operator->info = info;
    operator->input = input;
    operator->schema = NULL;
    operator->recordBuffer = NULL;
    operator->init = initSelection;
    operator->next = nextSelection;
    operator->close = closeSelection;

    return operator;
}

struct aggregateInfo {
    int8_t (*groupfunc)(const void* lastRecord, const void* record);  // record is in same group as last record
    sbitsAggrOp* operators;
    uint32_t numOps;
    void* lastRecordBuffer;
    uint16_t bufferSize;
    int8_t lastRecordUsable;  // Is the data in lastRecordBuffer usable?
};

void initAggregate(sbitsOperator* operator) {
    if (operator->input == NULL) {
        printf("ERROR: Aggregate operator needs an input operator\n");
        return;
    }

    // Init input
    operator->input->init(operator->input);

    struct aggregateInfo* info = operator->info;
    info->lastRecordUsable = 0;

    // Init output schema
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(sbitsSchema));
        if (operator->schema == NULL) {
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
            return;
        }
        operator->schema->numCols = info->numOps;
        operator->schema->columnSizes = malloc(info->numOps);
        if (operator->schema->columnSizes == NULL) {
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
            return;
        }
        for (uint8_t i = 0; i < info->numOps; i++) {
            operator->schema->columnSizes[i] = info->operators[i].colSize;
            info->operators[i].colNum = i;
        }
    }

    // Init buffers
    info->bufferSize = getRecordSizeFromSchema(operator->input->schema);
    if (operator->recordBuffer == NULL) {
        operator->recordBuffer = createBufferFromSchema(operator->schema);
        if (operator->recordBuffer == NULL) {
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
            return;
        }
    }
    if (info->lastRecordBuffer == NULL) {
        info->lastRecordBuffer = malloc(info->bufferSize);
        if (info->lastRecordBuffer == NULL) {
            printf("ERROR: Failed to malloc while initializing aggregate operator\n");
            return;
        }
    }
}

/**
 * @brief	Calculate an aggregate function over specified groups
 * @param	operator		An operator struct to pull input data from
 * @param	recordBuffer	Pre-allocated space for the operator to put the key. **NOT A RETURN VALUE**
 * @return	1 if another group was calculated, 0 if not.
 */
int8_t nextAggregate(sbitsOperator* operator) {
    struct aggregateInfo* info = operator->info;
    sbitsOperator* input = operator->input;

    // Reset each operator
    for (int i = 0; i < info->numOps; i++) {
        if (info->operators[i].reset != NULL) {
            info->operators[i].reset(info->operators + i);
        }
    }

    int8_t recordsInGroup = 0;

    // Check flag used to indicate whether the last record read has been added to a group
    if (info->lastRecordUsable) {
        recordsInGroup = 1;
        for (int i = 0; i < info->numOps; i++) {
            if (info->operators[i].add != NULL) {
                info->operators[i].add(info->operators + i, info->lastRecordBuffer);
            }
        }
    }

    int8_t exitType = 0;
    while (input->next(input)) {
        // Check if record is in the same group as the last record
        if (!info->lastRecordUsable || info->groupfunc(info->lastRecordBuffer, input->recordBuffer)) {
            recordsInGroup = 1;
            for (int i = 0; i < info->numOps; i++) {
                if (info->operators[i].add != NULL) {
                    info->operators[i].add(info->operators + i, input->recordBuffer);
                }
            }
        } else {
            exitType = 1;
            break;
        }

        // Save this record
        memcpy(info->lastRecordBuffer, input->recordBuffer, info->bufferSize);
        info->lastRecordUsable = 1;
    }

    if (!recordsInGroup) {
        return 0;
    }

    if (exitType == 0) {
        // Exited because ran out of records, so all read records have been added to a group
        info->lastRecordUsable = 0;
    }

    // Perform final compute on all operators
    for (int i = 0; i < info->numOps; i++) {
        if (info->operators[i].compute != NULL) {
            info->operators[i].compute(info->operators + i, operator->schema, operator->recordBuffer, info->lastRecordBuffer);
        }
    }

    // Put last read record into lastRecordBuffer
    memcpy(info->lastRecordBuffer, input->recordBuffer, info->bufferSize);

    return 1;
}

void closeAggregate(sbitsOperator* operator) {
    operator->input->close(operator->input);
    operator->input = NULL;
    sbitsFreeSchema(&operator->schema);
    free(((struct aggregateInfo*)operator->info)->lastRecordBuffer);
    free(operator->info);
    operator->info = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
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
sbitsOperator* createAggregateOperator(sbitsOperator* input, int8_t (*groupfunc)(const void* lastRecord, const void* record), sbitsAggrOp* operators, uint32_t numOps) {
    struct aggregateInfo* info = malloc(sizeof(struct aggregateInfo));
    if (info == NULL) {
        printf("ERROR: Failed to malloc while creating aggregate operator\n");
        return NULL;
    }

    info->groupfunc = groupfunc;
    info->operators = operators;
    info->numOps = numOps;
    info->lastRecordBuffer = NULL;

    sbitsOperator* operator= malloc(sizeof(sbitsOperator));
    if (operator== NULL) {
        printf("ERROR: Failed to malloc while creating aggregate operator\n");
        return NULL;
    }

    operator->info = info;
    operator->input = input;
    operator->schema = NULL;
    operator->recordBuffer = NULL;
    operator->init = initAggregate;
    operator->next = nextAggregate;
    operator->close = closeAggregate;

    return operator;
}

struct keyJoinInfo {
    sbitsOperator* input2;
    int8_t firstCall;
};

void initKeyJoin(sbitsOperator* operator) {
    struct keyJoinInfo* info = operator->info;
    sbitsOperator* input1 = operator->input;
    sbitsOperator* input2 = info->input2;

    // Init inputs
    input1->init(input1);
    input2->init(input2);

    sbitsSchema* schema1 = input1->schema;
    sbitsSchema* schema2 = input2->schema;

    // Check that join is compatible
    if (schema1->columnSizes[0] != schema2->columnSizes[0] || schema1->columnSizes[0] < 0 || schema2->columnSizes[0] < 0) {
        printf("ERROR: The first columns of the two tables must be the key and must have the size. Make sure you haven't projected them out.\n");
        return;
    }

    // Setup schema
    if (operator->schema == NULL) {
        operator->schema = malloc(sizeof(sbitsSchema));
        if (operator->schema == NULL) {
            printf("ERROR: Failed to malloc while initializing join operator\n");
            return;
        }
        operator->schema->numCols = schema1->numCols + schema2->numCols;
        operator->schema->columnSizes = malloc(operator->schema->numCols * sizeof(int8_t));
        if (operator->schema->columnSizes == NULL) {
            printf("ERROR: Failed to malloc while initializing join operator\n");
            return;
        }
        memcpy(operator->schema->columnSizes, schema1->columnSizes, schema1->numCols);
        memcpy(operator->schema->columnSizes + schema1->numCols, schema2->columnSizes, schema2->numCols);
    }

    // Allocate recordBuffer
    operator->recordBuffer = malloc(getRecordSizeFromSchema(operator->schema));
    if (operator->recordBuffer == NULL) {
        printf("ERROR: Failed to malloc while initializing join operator\n");
        return;
    }

    info->firstCall = 1;
}

/**
 * @brief	Performs equi-join on input keys
 */
int8_t nextKeyJoin(sbitsOperator* operator) {
    struct keyJoinInfo* info = operator->info;
    sbitsOperator* input1 = operator->input;
    sbitsOperator* input2 = info->input2;
    sbitsSchema* schema1 = input1->schema;
    sbitsSchema* schema2 = input2->schema;

    // We've already used this match
    void* record1 = input1->recordBuffer;
    void* record2 = input2->recordBuffer;

    int8_t colSize = abs(schema1->columnSizes[0]);

    while (1) {
        if (info->firstCall) {
            info->firstCall = 0;

            if (!input1->next(input1) || !input2->next(input2)) {
                // If this case happens, you goofed, but I'll handle it anyway
                return 0;
            }
        } else {
            // Advance the input with the smaller value
            int8_t comp = compareUnsignedNumbers(record1, record2, colSize);
            if (comp == 0) {
                // Move both forward because if they match at this point, they've already been matched
                if (!input1->next(input1) || !input2->next(input2)) {
                    return 0;
                }
            } else if (comp < 0) {
                // Move record 1 forward
                if (!input1->next(input1)) {
                    // We are out of records on one side. Given the assumption that the inputs are sorted, there are no more possible joins
                    return 0;
                }
            } else {
                // Move record 2 forward
                if (!input2->next(input2)) {
                    // We are out of records on one side. Given the assumption that the inputs are sorted, there are no more possible joins
                    return 0;
                }
            }
        }

        // See if these records join
        if (compareUnsignedNumbers(record1, record2, colSize) == 0) {
            // Copy both records into the output
            uint16_t record1Size = getRecordSizeFromSchema(schema1);
            memcpy(operator->recordBuffer, input1->recordBuffer, record1Size);
            memcpy((int8_t*)operator->recordBuffer + record1Size, input2->recordBuffer, getRecordSizeFromSchema(schema2));
            return 1;
        }
        // Else keep advancing inputs until a match is found
    }

    return 0;
}

void closeKeyJoin(sbitsOperator* operator) {
    struct keyJoinInfo* info = operator->info;
    sbitsOperator* input1 = operator->input;
    sbitsOperator* input2 = info->input2;
    sbitsSchema* schema1 = input1->schema;
    sbitsSchema* schema2 = input2->schema;

    input1->close(input1);
    input2->close(input2);

    sbitsFreeSchema(&operator->schema);
    free(operator->info);
    operator->info = NULL;
    free(operator->recordBuffer);
    operator->recordBuffer = NULL;
}

sbitsOperator* createKeyJoinOperator(sbitsOperator* input1, sbitsOperator* input2) {
    sbitsOperator* operator= malloc(sizeof(sbitsOperator));
    if (operator== NULL) {
        printf("ERROR: Failed to malloc while creating join operator\n");
        return NULL;
    }

    struct keyJoinInfo* info = malloc(sizeof(struct keyJoinInfo));
    if (info == NULL) {
        printf("ERROR: Failed to malloc while creating join operator\n");
        return NULL;
    }
    info->input2 = input2;

    operator->input = input1;
    operator->info = info;
    operator->recordBuffer = NULL;
    operator->schema = NULL;
    operator->init = initKeyJoin;
    operator->next = nextKeyJoin;
    operator->close = closeKeyJoin;

    return operator;
}

void countReset(sbitsAggrOp* aggrOp) {
    *(uint32_t*)aggrOp->state = 0;
}

void countAdd(sbitsAggrOp* aggrOp, const void* recordBuffer) {
    (*(uint32_t*)aggrOp->state)++;
}

void countCompute(sbitsAggrOp* aggrOp, sbitsSchema* schema, void* recordBuffer, const void* lastRecord) {
    // Put count in record
    memcpy((int8_t*)recordBuffer + getColPosFromSchema(schema, aggrOp->colNum), aggrOp->state, sizeof(uint32_t));
}

sbitsAggrOp* createCountAggregate() {
    sbitsAggrOp* aggrop = malloc(sizeof(sbitsAggrOp));
    aggrop->reset = countReset;
    aggrop->add = countAdd;
    aggrop->compute = countCompute;
    aggrop->state = malloc(sizeof(uint32_t));
    aggrop->colSize = 4;
    return aggrop;
}

void sumReset(sbitsAggrOp* aggrOp) {
    *(int64_t*)aggrOp->state = 0;
}

void sumAdd(sbitsAggrOp* aggrOp, const void* recordBuffer) {
    uint8_t colOffset = *((uint8_t*)aggrOp->state + sizeof(int64_t));
    int8_t colSize = *((int8_t*)aggrOp->state + sizeof(int64_t) + sizeof(uint8_t));
    int8_t isSigned = colSize < 0;
    colSize = min(abs(colSize), sizeof(int64_t));
    void* colPos = (int8_t*)recordBuffer + colOffset;
    if (isSigned) {
        // Get val to sum from record
        int64_t val = 0;
        memcpy(&val, colPos, colSize);
        // Extend two's complement sign to fill 64 bit number if val is negative
        int64_t sign = val & (128 << ((colSize - 1) * 8));
        if (sign != 0) {
            memset(((int8_t*)(&val)) + colSize, 0xff, sizeof(int64_t) - colSize);
        }
        (*(int64_t*)aggrOp->state) += val;
    } else {
        uint64_t val = 0;
        memcpy(&val, colPos, colSize);
        (*(uint64_t*)aggrOp->state) += val;
    }
}

void sumCompute(sbitsAggrOp* aggrOp, sbitsSchema* schema, void* recordBuffer, const void* lastRecord) {
    // Put count in record
    memcpy((int8_t*)recordBuffer + getColPosFromSchema(schema, aggrOp->colNum), aggrOp->state, sizeof(int64_t));
}

sbitsAggrOp* createSumAggregate(uint8_t colOffset, int8_t colSize) {
    sbitsAggrOp* aggrop = malloc(sizeof(sbitsAggrOp));
    aggrop->reset = sumReset;
    aggrop->add = sumAdd;
    aggrop->compute = sumCompute;
    aggrop->state = malloc(2 * sizeof(int8_t) + sizeof(int64_t));
    *((uint8_t*)aggrop->state + sizeof(int64_t)) = colOffset;
    *((int8_t*)aggrop->state + sizeof(int64_t) + sizeof(uint8_t)) = colSize;
    aggrop->colSize = -8;
    return aggrop;
}

/**
 * @brief	Completely free a chain of operators recursively. Does not recursively free any pointer contained in `sbitsOperator::info`
 */
void sbitsFreeOperatorRecursive(sbitsOperator** operator) {
    if ((*operator)->input != NULL) {
        sbitsFreeOperatorRecursive(&(*operator)->input);
    }
    if ((*operator)->info != NULL) {
        free((*operator)->info);
        (*operator)->info = NULL;
    }
    if ((*operator)->schema != NULL) {
        sbitsFreeSchema(&(*operator)->schema);
    }
    if ((*operator)->recordBuffer != NULL) {
        free((*operator)->recordBuffer);
        (*operator)->recordBuffer = NULL;
    }
    free(*operator);
    (*operator) = NULL;
}
