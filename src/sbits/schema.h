#ifndef SBITS_SCHEMA_H_
#define SBITS_SCHEMA_H_

#include <stdint.h>

#define SBITS_COLUMN_SIGNED 0
#define SBITS_COLUMN_UNSIGNED 1
#define SBITS_IS_COL_SIGNED(colSize) (colSize < 0 ? 1 : 0)

/**
 * @brief	A struct to desribe the number and sizes of attributes contained in the data of a sbits table
 */
typedef struct {
    uint8_t numCols;      // The number of columns in the table
    int8_t* columnSizes;  // A list of the sizes, in bytes, of each column. Negative numbers indicate signed columns while positive indicate an unsigned column
} sbitsSchema;

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

/**
 * @brief	Uses schema to determine the length of buffer to allocate
 */
void* createBufferFromSchema(sbitsSchema* schema);

/**
 * @brief	Deep copy schema and return a pointer to the copy
 */
sbitsSchema* copySchema(const sbitsSchema* schema);

/**
 * @brief	Finds byte offset of the column from the beginning of the record
 */
uint16_t getColPosFromSchema(sbitsSchema* schema, uint8_t colNum);

/**
 * @brief	Calculates record size from schema
 */
uint16_t getRecordSizeFromSchema(sbitsSchema* schema);

void printSchema(sbitsSchema* schema);

#endif
