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

typedef struct sbitsOperator_ {
    struct sbitsOperator_* input;
    int8_t (*func)(sbitsState* state, void* info, void* key, void* data);
    void* info;
} sbitsOperator;

/**
 * @brief	Execute a function on each return of an iterator
 * @param	state	The sbitsState struct with the database to query
 * @param	it		An iterator that has been initialized to query the relevant data
 * @param	func	The function to run on each (key, data) pair returned by the iterator. Should return 0 or 1 to indicate whether exec should return the pair.
 * @param	key		Pre-allocated space for the key
 * @param	data	Pre-allocated space for the data
 * @return	1 if another (key, data) pair was returned, 0 if there are no more pairs to return
 */
// int8_t exec(sbitsState* state, sbitsIterator* it, int8_t (*func)(void* key, void* data), void* key, void* data);
int8_t exec(sbitsState* state, sbitsOperator* operator, void * key, void* data);

/**
 * @brief	Calculate an aggregate function over specified groups
 * @param	state		The sbitsState to iterate over
 * @param	it			An iterator set up to return the relevant range of data
 * @param	groupfunc	A function that returns whether or not the `key` is part of the same group as the `lastkey`. Assumes that groups are always next to each other when read in.
 * @param	operators	An array of operators, each of which will be updated with each record read from the iterator
 * @param	numOps		The number of sbitsAggrOps in `operators`
 * @param	key			Pre-allocated space for the iterator to put the key. **NOT A RETURN VALUE**
 * @param	data		Pre-allocated space for the iterator to put the data. **NOT A RETURN VALUE**
 */
int8_t aggroup(sbitsState* state, sbitsIterator* it, int8_t (*groupfunc)(void* lastkey, void* key), sbitsAggrOp* operators, uint32_t numOps, void* key, void* data);

#endif
