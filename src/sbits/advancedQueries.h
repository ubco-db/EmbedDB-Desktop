#ifndef _ADVANCEDQUERIES_H
#define _ADVANCEDQUERIES_H

#include "sbits.h"

#define SELECT_GT 0
#define SELECT_LT 1
#define SELECT_GTE 2
#define SELECT_LTE 3
#define SBITS_COLUMN_SIGNED 0
#define SBITS_COLUMN_UNSIGNED 1
#define SBITS_COL_IS_SIGNED(colSize) (colSize < 0 ? 1 : 0)

/**
 * @brief	A struct to desribe the number and sizes of attributes contained in the data of a sbits table
 */
typedef struct {
    uint8_t numCols;      // The number of columns in the table
    int8_t* columnSizes;  // A list of the sizes, in bytes, of each column
} sbitsSchema;

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
    void (*add)(void* state, void* record);

    /**
     * @brief	Finalize aggregate result. Is called once right before aggroup returns.
     */
    void (*compute)(void* state);

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
     * @param	schema			The schema of the data in `recordBuffer`. If this function makes any changes in `recordBuffer` that affect the schema, it should be reflected in this variable.
     * @param	info			A pre-allocated memory area that can be loaded with any extra parameters that the function needs to operate (e.g. column numbers or selection predicates)
     * @param	recordBuffer	The key to perform the function on
     * @return	Returns 0 or 1 to indicate whether this tuple should continue to be processed/returned
     */
    int8_t (*func)(sbitsSchema* schema, void* info, void* recordBuffer);

    /**
     * @brief	A pre-allocated memory area that can be loaded with any extra parameters that the function needs to operate (e.g. column numbers or selection predicates)
     */
    void* info;
} sbitsOperator;

/**
 * @brief	Execute a function on each return of an iterator
 * @param	operator		An operator struct containing the input operator and function to run on the data
 * @param	schema			Pre-allocated space for the schema
 * @param	recordBuffer	Pre-allocated space for the whole record (key & data)
 * @return	1 if another (key, data) pair was returned, 0 if there are no more pairs to return
 */
int8_t exec(sbitsOperator* operator, sbitsSchema * schema, void* recordBuffer);

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
int8_t aggroup(sbitsOperator* input, int8_t (*groupfunc)(void* lastRecord, void* record), sbitsAggrOp* operators, uint32_t numOps, sbitsSchema* schema, void* recordBuffer, uint8_t bufferSize);

/**
 * @brief	Create an sbitsSchema from a list of column sizes including both key and data
 * @param	numCols			The total number of key & data columns in table
 * @param	colSizes		An array with the size of each column. Max size is 127
 * @param	colSignedness	An array describing if the data in the column is SBITS_COLUMNN_SIGNED or SBITS_COLUMN_UNSIGNED
 */
sbitsSchema* sbitsCreateSchema(uint8_t numCols, int8_t* colSizes, int8_t* colSignedness);

/**
 * @brief	Free a schema. Sets the schema pointer to NULL.
 */
void sbitsFreeSchema(sbitsSchema** schema);

///////////////////////////////////////////
// Pre-built functions for basic queries //
///////////////////////////////////////////

int8_t tableScan(sbitsSchema* schema, void* info, void* recordBuffer);
void* createTableScanInfo(sbitsState* state, sbitsIterator* it, sbitsSchema* baseSchema);

int8_t projectionFunc(sbitsSchema* schema, void* info, void* recordBuffer);
void* createProjectionInfo(uint8_t numCols, uint8_t* cols);

/**
 * @brief	A basic selection function that can filter on the data column
 */
int8_t selectionFunc(sbitsSchema* schema, void* info, void* recordBuffer);
void* createSelectinfo(int8_t colNum, int8_t operation, void* compVal);

#endif
