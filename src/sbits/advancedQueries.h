#ifndef _ADVANCEDQUERIES_H
#define _ADVANCEDQUERIES_H

#include "sbits.h"
#include "schema.h"

#define SELECT_GT 0
#define SELECT_LT 1
#define SELECT_GTE 2
#define SELECT_LTE 3

typedef struct {
    /**
     * @brief	Resets the state
     */
    void (*reset)(void* state);

    /**
     * @brief	Adds another record to the group and updates the state
     * @param	state	The state tracking the value of the aggregate function e.g. sum
     * @param	record	The record being added
     */
    void (*add)(void* state, const void* record);

    /**
     * @brief	Finalize aggregate result into the record buffer and modify the schema accordingly. Is called once right before aggroup returns.
     */
    void (*compute)(void* state, sbitsSchema* schema, void* recordBuffer, const void* lastRecord);

    /**
     * @brief	A user-malloced space where the operator saves its state. E.g. a sum operator might have 4 bytes allocated to store the sum of all data
     */
    void* state;
} sbitsAggrOp;

typedef struct sbitsOperator {
    /**
     * @brief	The input operator to this operator. exec() will read one tuple at a time from it. If it is NULL, this operator will be considered to be a base level operator capable of a table scan or similar.
     */
    struct sbitsOperator* input;

    /**
     * @brief	A function to run on each tuple extracted from `input` if input is not NULL. If `input` is NULL, then it should be capable of table scan or similar.
     * @param	operator		This operator
     * @param	recordBuffer	The key to perform the function on
     * @return	Returns 0 or 1 to indicate whether this tuple should continue to be processed/returned
     */
    int8_t (*func)(struct sbitsOperator* operator, void * recordBuffer);
    // int8_t (*func)(sbitsSchema* schema, void* info, void* recordBuffer);

    /**
     * @brief	A pre-allocated memory area that can be loaded with any extra parameters that the function needs to operate (e.g. column numbers or selection predicates)
     */
    void* info;

    /**
     * @brief	The output schema of this operator
     */
    sbitsSchema* schema;
} sbitsOperator;

/**
 * @brief	Execute a function on each return of an iterator
 * @param	operator		An operator struct containing the input operator and function to run on the data
 * @param	recordBuffer	Pre-allocated space for the whole record (key & data)
 * @return	1 if another (key, data) pair was returned, 0 if there are no more pairs to return
 */
int8_t exec(sbitsOperator* operator, void * recordBuffer);

int8_t join(sbitsOperator* op1, void* recordBuffer1, sbitsOperator* op2, void* recordBuffer2);

/**
 * @brief	Completely free a chain of operators recursively. Does not recursively free any pointer contained in `sbitsOperator::info`
 */
void sbitsDestroyOperatorRecursive(sbitsOperator** operator);

///////////////////////////////////////////
// Pre-built functions for basic queries //
///////////////////////////////////////////

int8_t tableScan(sbitsOperator* operator, void * recordBuffer);
void* createTableScanInfo(sbitsState* state, sbitsIterator* it);

int8_t projectionFunc(sbitsOperator* operator, void * recordBuffer);
void* createProjectionInfo(uint8_t numCols, uint8_t* cols);

int8_t selectionFunc(sbitsOperator* operator, void * recordBuffer);
void* createSelectinfo(int8_t colNum, int8_t operation, void* compVal);

/**
 * @brief	Calculate an aggregate function over specified groups
 * @param	operator		An operator struct to pull input data from
 * @param	recordBuffer	Pre-allocated space for the operator to put the key. **NOT A RETURN VALUE**
 * @return	1 if another group was calculated, 0 if not.
 */
int8_t aggregateFunc(sbitsOperator* operator, void * recordBuffer);

/**
 * @brief	Create the info for an aggregate operator
 * @param	groupfunc		A function that returns whether or not the `key` is part of the same group as the `lastkey`. Assumes that groups are always next to each other when read in.
 * @param	operators		An array of operators, each of which will be updated with each record read from the iterator
 * @param	numOps			The number of sbitsAggrOps in `operators`
 * @param	lastRecordBuffer	A secondary buffer needed to store the last key that was read for the purpose of comparing it to the record that was just read. Needs to be the same size as `recordBuffer`
 * @param	bufferSize		The length (in bytes) of `recordBuffer`
 * @return	Returns a pointer to the info object to be put into a sbitsOperator
 */
void* createAggregateInfo(int8_t (*groupfunc)(const void* lastRecord, const void* record), sbitsAggrOp* operators, uint32_t numOps, void* lastRecordBuffer, uint8_t bufferSize);

#endif
