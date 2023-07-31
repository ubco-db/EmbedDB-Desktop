#ifndef _ADVANCEDQUERIES_H
#define _ADVANCEDQUERIES_H

#include "sbits.h"
#include "schema.h"

#define SELECT_GT 0
#define SELECT_LT 1
#define SELECT_GTE 2
#define SELECT_LTE 3
#define SELECT_EQ 4
#define SELECT_NEQ 5

typedef struct sbitsAggrOp {
    /**
     * @brief	Resets the state
     */
    void (*reset)(struct sbitsAggrOp* aggrOp);

    /**
     * @brief	Adds another record to the group and updates the state
     * @param	state	The state tracking the value of the aggregate function e.g. sum
     * @param	record	The record being added
     */
    void (*add)(struct sbitsAggrOp* aggrOp, const void* record);

    /**
     * @brief	Finalize aggregate result into the record buffer and modify the schema accordingly. Is called once right before aggroup returns.
     */
    void (*compute)(struct sbitsAggrOp* aggrOp, sbitsSchema* schema, void* recordBuffer, const void* lastRecord);

    /**
     * @brief	A user-malloced space where the operator saves its state. E.g. a sum operator might have 4 bytes allocated to store the sum of all data
     */
    void* state;

    /**
     * @brief	How many bytes will the compute insert into the record
     */
    int8_t colSize;

    /**
     * @brief	Which column number should compute write to
     */
    uint8_t colNum;
} sbitsAggrOp;

typedef struct sbitsOperator {
    /**
     * @brief	The input operator to this operator
     */
    struct sbitsOperator* input;

    /**
     * @brief	Initialize the operator. Usually includes setting/calculating the output schema, allocating buffers, etc. Recursively inits input operator as its first action.
     */
    void (*init)(struct sbitsOperator* operator);

    /**
     * @brief	Puts the next tuple to be outputed by this operator into @c operator->recordBuffer. Needs to call next on the input operator if applicable
     * @return	Returns 0 or 1 to indicate whether a new tuple was outputted to operator->recordBuffer
     */
    int8_t (*next)(struct sbitsOperator* operator);

    /**
     * @brief	Recursively closes this operator and its input operator. Frees anything allocated in init.
     */
    void (*close)(struct sbitsOperator* operator);

    /**
     * @brief	A pre-allocated memory area that can be loaded with any extra parameters that the function needs to operate (e.g. column numbers or selection predicates)
     */
    void* info;

    /**
     * @brief	The output schema of this operator
     */
    sbitsSchema* schema;

    /**
     * @brief	The output record of this operator
     */
    void* recordBuffer;
} sbitsOperator;

/**
 * @brief	Extract a record from an operator
 * @return	1 if a record was returned, 0 if there are no more pairs to return
 */
int8_t exec(sbitsOperator* operator);

/**
 * @brief	Completely free a chain of operators recursively after it's already been closed.
 */
void sbitsFreeOperatorRecursive(sbitsOperator** operator);

///////////////////////////////////////////
// Pre-built functions for basic queries //
///////////////////////////////////////////

/**
 * @brief	Used as the bottom operator that will read records from the database
 * @param	state		The state associated with the database to read from
 * @param	it			An initialized iterator setup to read relevent records for this query
 * @param	baseSchema	The schema of the database being read from
 */
sbitsOperator* createTableScanOperator(sbitsState* state, sbitsIterator* it, sbitsSchema* baseSchema);

/**
 * @brief	Creates an operator capable of projecting the specified columns. Cannot re-order columns
 * @param	input	The operator that this operator can pull records from
 * @param	numCols	How many columns will be in the final projection
 * @param	cols	The indexes of the columns to be outputted. *Zero indexed*
 */
sbitsOperator* createProjectionOperator(sbitsOperator* input, uint8_t numCols, uint8_t* cols);

/**
 * @brief	Creates an operator that selects records based on simple selection rules
 * @param	input		The operator that this operator can pull records from
 * @param	colNum		The index (zero-indexed) of the column base the select on
 * @param	operation	A constant representing which comparison operation to perform. (e.g. SELECT_GT, SELECT_EQ, etc)
 * @param	compVal		A pointer to the value to compare with. Make sure the size of this is the same number of bytes as is described in the schema
 */
sbitsOperator* createSelectionOperator(sbitsOperator* input, int8_t colNum, int8_t operation, void* compVal);

/**
 * @brief	Create the info for an aggregate operator
 * @param	input			The operator that this operator can pull records from
 * @param	groupfunc		A function that returns whether or not the @c record is part of the same group as the @c lastRecord. Assumes that groups are always next to each other/sorted when read in (i.e. Groups need to be 1122333, not 13213213)
 * @param	operators		An array of aggregate operators, each of which will be updated with each record read from the iterator
 * @param	numOps			The number of sbitsAggrOps in @c operators
 */
sbitsOperator* createAggregateOperator(sbitsOperator* input, int8_t (*groupfunc)(const void* lastRecord, const void* record), sbitsAggrOp* operators, uint32_t numOps);

int8_t nextJoin(sbitsOperator* operator);

#endif
