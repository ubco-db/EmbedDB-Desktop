#ifndef _ADVANCEDQUERIES_H
#define _ADVANCEDQUERIES_H

#include "sbits.h"

typedef struct {
    /**
     * @brief	Resets the state
     */
    void (*reset)(void* state);

    /**
     * @brief	Adds another record to the group and updates the state
     * @param	state	The state tracking the value of the aggregate function e.g. sum
     * @param	key		The key being added
     * @param	data	The data being added
     */
    void (*add)(void* state, void* key, void* data);

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
     * @param	state	The sbitsState defining the table being read
     * @param	info	A pre-allocated memory area that can be loaded with any extra parameters that the function needs to operate (e.g. column numbers or selection predicates)
     * @param	key		The key to perform the function on
     * @param	data	The data to perform the function on
     * @return	Returns 0 or 1 to indicate whether this tuple should continue to be processed/returned
     */
    int8_t (*func)(sbitsState* state, void* info, void* key, void* data);

    /**
     * @brief	A pre-allocated memory area that can be loaded with any extra parameters that the function needs to operate (e.g. column numbers or selection predicates)
     */
    void* info;
} sbitsOperator;

/**
 * @brief	Execute a function on each return of an iterator
 * @param	state		The sbitsState struct with the database to query
 * @param	operator	An operator struct containing the input operator and function to run on the data
 * @param	key			Pre-allocated space for the key
 * @param	data		Pre-allocated space for the data
 * @return	1 if another (key, data) pair was returned, 0 if there are no more pairs to return
 */
int8_t exec(sbitsState* state, sbitsOperator* operator, void * key, void* data);

/**
 * @brief	Calculate an aggregate function over specified groups
 * @param	state		The sbitsState to iterate over
 * @param	input		An operator struct to pull input data from
 * @param	groupfunc	A function that returns whether or not the `key` is part of the same group as the `lastkey`. Assumes that groups are always next to each other when read in.
 * @param	operators	An array of operators, each of which will be updated with each record read from the iterator
 * @param	numOps		The number of sbitsAggrOps in `operators`
 * @param	key			Pre-allocated space for the iterator to put the key. **NOT A RETURN VALUE**
 * @param	data		Pre-allocated space for the iterator to put the data. **NOT A RETURN VALUE**
 */
int8_t aggroup(sbitsState* state, sbitsOperator* input, int8_t (*groupfunc)(void* lastkey, void* key), sbitsAggrOp* operators, uint32_t numOps, void* key, void* data);

#endif
