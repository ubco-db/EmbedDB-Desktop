#include "advancedQueries.h"

#include <string.h>

/**
 * @brief	Execute a function on each return of an iterator
 * @param	state		The sbitsState struct with the database to query
 * @param	operator	An operator struct containing the input operator and function to run on the data
 * @param	key			Pre-allocated space for the key
 * @param	data		Pre-allocated space for the data
 * @return	1 if another (key, data) pair was returned, 0 if there are no more pairs to return
 */
int8_t exec(sbitsState* state, sbitsOperator* operator, void * key, void* data) {
    if (operator->input == NULL) {
        // Bottom level operator such as a table scan i.e. sbitsIterator
        return operator->func(state, operator->info, key, data);
    } else {
        // Execute `func` on top of the output of `input`
        while (exec(state, operator->input, key, data)) {
            if (operator->func(state, operator->info, key, data)) {
                return 1;
            }
        }
        return 0;
    }
}

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
int8_t aggroup(sbitsState* state, sbitsOperator* input, int8_t (*groupfunc)(void* lastkey, void* key), sbitsAggrOp* operators, uint32_t numOps, void* key, void* data) {
    // Reset each operator
    for (int i = 0; i < numOps; i++) {
        operators[i].reset(operators[i].state);
    }

    // Check if this call is the first for this query
    uint64_t keySizeMask = UINT64_MAX;
    keySizeMask >>= state->keySize * 8;
    keySizeMask <<= state->keySize * 8;
    uint64_t lastkey = 0;
    memcpy(&lastkey, key, state->keySize);
    int8_t isFirstCall = ~(lastkey | keySizeMask) == 0;

    // If it is not the first call, need to add() the last key
    if (!isFirstCall) {
        for (int i = 0; i < numOps; i++) {
            operators[i].add(operators[i].state, key, data);
        }
    }

    // Get next record
    int8_t gotRecords = 0;
    while (exec(state, input, key, data)) {
        gotRecords = 1;
        // Check if record is in the same group as the last record
        if (isFirstCall || groupfunc(&lastkey, key)) {
            isFirstCall = 0;
            for (int i = 0; i < numOps; i++) {
                operators[i].add(operators[i].state, key, data);
            }
        } else {
            break;
        }

        // Save this key
        lastkey = 0;
        memcpy(&lastkey, key, state->keySize);
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
