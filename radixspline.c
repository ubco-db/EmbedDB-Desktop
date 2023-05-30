/******************************************************************************/
/**
 * @file		radixspline.c
 * @author		Ramon Lawrence
 * @brief		Implementation of radix spline for embedded devices.
 *                         Based on "RadixSpline: a single-pass learned index" by
 *                         A. Kipf, R. Marcus, A. van Renen, M. Stoian, A. Kemper,
 *                         T. Kraska, and T. Neumann
 *                         https://github.com/learnedsystems/RadixSpline
 * @copyright	Copyright 2021
 *                         The University of British Columbia,
 *                         Ramon Lawrence
 * @par Redistribution and use in source and binary forms, with or without
 *         modification, are permitted provided that the following conditions are
 * met:
 *
 * @par 1.Redistributions of source code must retain the above copyright notice,
 *         this list of conditions and the following disclaimer.
 *
 * @par 2.Redistributions in binary form must reproduce the above copyright notice,
 *         this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * @par 3.Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *         AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *         ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *         CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *         SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *         INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *         CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *         ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/

#include "radixspline.h"

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

/**
 * @brief   Build the radix table
 * @param   rsdix       Radix spline structure
 * @param   data        Data points to be indexed
 * @param   dataSize    Size of data
 */
void radixsplineBuild(radixspline *rsidx, int32_t *data, uint32_t dataSize) {
    // radixsplinePrint(rsidx);
    rsidx->pointsSeen = 0;
    rsidx->prevPrefix = 0;

    for (id_t i = 0; i < dataSize; i++) {
        radixsplineAddPoint(rsidx, data[i]);
    }
}

/**
 * @brief   Rebuild the radix table with new shift amount
 * @param   rsdix       Radix spline structure
 * @param   spl         Spline structure
 * @param   radixSize   Size of radix table
 * @param   shiftAmount Difference in shift amount between current radix table and desired radix table
 */
void radixsplineRebuild(radixspline *rsidx, int8_t radixSize, int8_t shiftAmount) {
    // radixsplinePrint(rsidx);
    rsidx->prevPrefix = rsidx->prevPrefix >> shiftAmount;

    for (id_t i = 0; i < rsidx->size / pow(2, shiftAmount); i++) {
        rsidx->table[i] = rsidx->table[(i << shiftAmount)];
    }
    for (id_t i = rsidx->size / pow(2, shiftAmount); i < rsidx->size; i++) {
        rsidx->table[i] = INT32_MAX;
    }
}

/**
 * @brief	Add a point to be indexed by the radix spline structure
 * @param	rsdix	Radix spline structure
 * @param	key		New point to be indexed by radix spline
 */
void radixsplineAddPoint(radixspline *rsidx, uint32_t key) {
    splineAdd(rsidx->spl, key);

    // Return if not using Radix table
    if (rsidx->radixSize == 0) {
        return;
    }

    // Add spline point
    uint32_t lastKey = rsidx->spl->lastKey;

    // Determine if need to update radix table based on adding point to spline
    if (rsidx->spl->count <= rsidx->pointsSeen)
        return; // Nothing to do

    // take the last point that was added to spline
    key = rsidx->spl->points[rsidx->spl->count - 1].x;

    // Initialize table and minKey on first key added
    if (rsidx->pointsSeen == 0) {
        rsidx->table = (uint32_t *)malloc((sizeof(uint32_t)) * rsidx->size);
        int32_t counter;
        for (counter = 1; counter < rsidx->size; counter++) {
            rsidx->table[counter] = INT32_MAX;
        }
        rsidx->minKey = key;
    }

    // Check if prefix will fit in 32 bits
    uint32_t prefixSize = __builtin_clz(key - rsidx->minKey);
    int8_t newShiftSize;
    if (32 - prefixSize < (uint32_t)rsidx->radixSize) // 32-bit key
        newShiftSize = 0;
    else
        newShiftSize = (32 - prefixSize) - rsidx->radixSize;

    // if the shift size changes, need to remake table from scratch using new
    // shift size
    if (newShiftSize > rsidx->shiftSize) {
        radixsplineRebuild(rsidx, rsidx->radixSize,
                           newShiftSize - rsidx->shiftSize);
        rsidx->shiftSize = newShiftSize;
    }

    id_t prefix;
    prefix = (key - rsidx->minKey) >> rsidx->shiftSize;
    if (prefix !=
        rsidx->prevPrefix) { // printf("New prefix: %lu \t"TO_BINARY_PATTERN"
        // "TO_BINARY_PATTERN"\n", prefix,
        // TO_BINARY((uint8_t)(prefix>>8)),
        // TO_BINARY((uint8_t) prefix));
        id_t pr;
        for (pr = rsidx->prevPrefix; pr < prefix; pr++)
            rsidx->table[pr] = rsidx->pointsSeen;

        rsidx->prevPrefix = prefix;
    }

    rsidx->table[prefix] = rsidx->pointsSeen;

    rsidx->pointsSeen++;
    rsidx->dataSize = rsidx->spl->currentPointLoc;

    // radixsplinePrint((*rsidx));
}

/**
 * @brief	Initialize an empty radix spline index of given size
 * @param	rsdix		Radix spline structure
 * @param	spl			Spline structure
 * @param	radixSize	Size of radix table
 */
void radixsplineInit(radixspline *rsidx, spline *spl, int8_t radixSize) {
    rsidx->spl = spl;
    rsidx->radixSize = radixSize;
    rsidx->dataSize = 0;
    rsidx->shiftSize = 0;
    rsidx->size = pow(2, radixSize);

    /* Determine the prefix size (shift bits) based on min and max keys */
    rsidx->minKey = spl->points[0].x;

    /* Initialize points seen */
    rsidx->pointsSeen = 0;
    rsidx->prevPrefix = 0;
}

/**
 * @brief	Performs a recursive binary search for a given value
 * @param	arr		Array to search through
 * @param	low		Lower search bound
 * @param	high	Higher search bound
 * @param	x		Search term
 */
size_t radixBinarySearch(point *arr, int low, int high, int x) {
    int32_t mid;
    if (high >= low) {
        mid = low + (high - low) / 2;

        if (arr[mid].x >= x && arr[mid - 1].x <= x)
            return mid;

        if (arr[mid].x > x)
            return radixBinarySearch(arr, low, mid - 1, x);

        return radixBinarySearch(arr, mid + 1, high, x);
    }

    mid = low + (high - low) / 2;
    if (mid >= high) {
        return high;
    } else {
        return low;
    }
}

/**
 * @brief	Initialize a radix spline index of given size using pre-built spline structure.
 * @param	rsdix		Radix spline structure
 * @param	spl			Spline structure
 * @param	radixSize	Size of radix table
 * @param	data		Data points to be indexed
 * @param	dataSize	Size of data
 */
void radixsplineInitBuild(radixspline *rsidx, spline *spl, uint32_t radixSize,
                          int32_t *data, uint32_t dataSize) {
    radixsplineInit(rsidx, spl, radixSize);
    radixsplineBuild(rsidx, data, dataSize);
}

/**
 * @brief	Returns the radix index that is end of spline segment containing
 * key using radix table.
 * @param	rsidx	Radix spline structure
 * @param	key		Search key
 */
size_t radixsplineGetEntry(radixspline *rsidx, id_t key) {
    /* Use radix table to find range of spline points */
    id_t prefix = (key - rsidx->minKey) >> rsidx->shiftSize;

    uint32_t begin;
    uint32_t end;

    // Determine end, use next higher radix point if within bounds, unless key
    // is exactly prefix
    if (key == (prefix << rsidx->shiftSize)) {
        end = rsidx->table[prefix];
    } else {
        if ((prefix + 1) < rsidx->size) {
            end = rsidx->table[prefix + 1];
        } else {
            end = rsidx->table[rsidx->size - 1];
        }
    }

    // check end is in bounds since radix table values are initiated to INT_MAX
    if (end >= rsidx->spl->count) {
        end = rsidx->spl->count - 1;
    }

    // use previous adjacent radix point for lower bounds
    if (prefix == 0) {
        begin = 0;
    } else {
        begin = rsidx->table[prefix - 1];
    }

    return radixBinarySearch(rsidx->spl->points, begin, end, key);
}

/**
 * @brief	Returns the radix index that is end of spline segment containing
 * key using binary search.
 * @param	rsidx	Radix spline structure
 * @param	key		Search key
 */
size_t radixsplineGetEntryBinarySearch(radixspline *rsidx, id_t key) {
    return radixBinarySearch(rsidx->spl->points, 0, rsidx->spl->count - 1, key);
}

/**
 * @brief	Estimate location of key in data using spline points.
 * @param	rsidx	Radix spline structure
 * @param	key		Search key
 * @param	index	Spline index entry
 */
size_t radixsplineEstimateLocation(radixspline *rsidx, id_t key) {
    if (key < rsidx->minKey)
        return 0;

    size_t index;
    if (rsidx->radixSize == 0) {
        /* Get index using binary search */
        index = radixsplineGetEntryBinarySearch(rsidx, key);
    } else {
        /* Get index using radix table */
        index = radixsplineGetEntry(rsidx, key);
    }

    /* Interpolate between two spline points */
    point down = rsidx->spl->points[index - 1];
    point up = rsidx->spl->points[index];

    /* Keydiff * slope + y */
    size_t val = (key - down.x) * ((double)(up.y - down.y) / (up.x - down.x)) + down.y;
    return val > up.y ? up.y : val;
}

/**
 * @brief	Finds a value using index. Returns predicted location and low and high error bounds.
 * @param	rsidx	Radix spline structure
 * @param	key		Search key
 * @param	loc		Predicted location
 * @param	low		Low bound on predicted location
 * @param	high	High bound on predicted location
 */
id_t radixsplineFind(radixspline *rsidx, id_t key, id_t *loc, id_t *low, id_t *high) {
    /* Estimate location */
    *loc = radixsplineEstimateLocation(rsidx, key);

    /* Set error bounds based on maxError from spline construction */
    *low = (rsidx->spl->maxError > *loc) ? 0 : *loc - rsidx->spl->maxError;
    point lastSplinePoint = rsidx->spl->points[rsidx->spl->count - 1];
    *high = (*loc + rsidx->spl->maxError > lastSplinePoint.x) ? lastSplinePoint.x : *loc + rsidx->spl->maxError;

    return *loc;
}

/**
 * @brief	Print radix spline structure.
 * @param	rsidx	Radix spline structure
 */
void radixsplinePrint(radixspline *rsidx) {
    if (rsidx == NULL || rsidx->radixSize == 0) {
        printf("No radix spline index to print.\n");
        return;
    }

    printf("Radix table (%lu):\n", rsidx->size);
    // for (id_t i=0; i < 20; i++)
    for (id_t i = 0; i < rsidx->size; i++) {
        printf("[" TO_BINARY_PATTERN "] ", TO_BINARY((uint8_t)(i)));
        printf("(%lu): --> %lu\n", (i << rsidx->shiftSize) + rsidx->minKey, rsidx->table[i]);
    }
    printf("\n");
}

/**
 * @brief	Returns size of radix spline index structure in bytes
 * @param	rsidx	Radix spline structure
 */
size_t radixsplineSize(radixspline *rsidx) {
    return sizeof(rsidx) + rsidx->size * sizeof(uint32_t) + splineSize(rsidx->spl);
}
