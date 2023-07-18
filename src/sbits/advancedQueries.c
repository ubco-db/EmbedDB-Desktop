#include "advancedQueries.h"

#include <string.h>

/**
 * @brief	Execute a function on each return of an iterator
 * @param	state	The sbitsState struct with the database to query
 * @param	it		An iterator that has been initialized to query the relevant data
 * @param	func	The function to run on each (key, data) pair returned by the iterator. Should return 0 or 1 to indicate whether exec should return the pair.
 * @param	key		Pre-allocated space for the key
 * @param	data	Pre-allocated space for the data
 * @return	1 if another (key, data) pair was returned, 0 if there are no more pairs to return
 */
int8_t exec(sbitsState* state, sbitsIterator* it, int8_t (*func)(void* key, void* data), void* key, void* data) {
    while (sbitsNext(state, it, key, data)) {
        if (func(key, data)) {
            return 1;
        }
    }
    return 0;
}

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
int8_t aggroup(sbitsState* state, sbitsIterator* it, int8_t (*groupfunc)(void* lastkey, void* key), sbitsAggrOp* operators, uint32_t numOps, void* key, void* data) {
    // Reset each operator
    for (int i = 0; i < numOps; i++) {
        operators[i].reset(operators[i].state);
    }

    uint64_t lastkey = UINT64_MAX;
    // Get next record
    while (sbitsNext(state, it, key, data)) {
        // printf("Key: %lu  Data: %d %d %d\n", *(uint32_t*)key, *(int32_t*)data, *(int32_t*)((int8_t*)data + 4), *(int32_t*)((int8_t*)data + 8));
        // Check if record is in the same group as the last record
        if (lastkey == UINT64_MAX || groupfunc(&lastkey, key)) {
            for (int i = 0; i < numOps; i++) {
                operators[i].add(operators[i].state, key, data);
            }
        } else {
            goto returnGroup;
        }

        // Save this key
        memcpy(&lastkey, key, state->keySize);
    }

    // If true, then the iterator did not return any records on this call of aggroup()
    if (lastkey == UINT64_MAX) {
        return 0;
    }

returnGroup:
    // Move iterator back a record because the last record we read belongs to the next group
    if (it->nextDataRec != 0) {
        it->nextDataRec--;
    } else {
        it->nextDataPage--;
        it->nextDataRec = state->maxRecordsPerPage - 1;
    }

    // Perform final compute on all operators
    for (int i = 0; i < numOps; i++) {
        operators[i].compute(operators[i].state);
    }

    return 1;
}
