#ifndef _ADVANCEDQUERIES_H
#define _ADVANCEDQUERIES_H

#include "sbits.h"

/**
 * @brief	Execute a function on each return of an iterator
 * @param	state	The sbitsState struct with the database to query
 * @param	it		An iterator that has been initialized to query the relevant data
 * @param	func	The function to run on each (key, data) pair returned by the iterator. Should return 0 or 1 to indicate whether exec should return the pair.
 * @param	key		Pre-allocated space for the key
 * @param	data	Pre-allocated space for the data
 * @return	1 if another (key, data) pair was returned, 0 if there are no more pairs to return
 */
int8_t exec(sbitsState* state, sbitsIterator* it, int8_t (*func)(void* key, void* data), void* key, void* data);

#endif
