#ifndef embedDB_SCHEMA_H_
#define embedDB_SCHEMA_H_

#include <stdint.h>

#define embedDB_COLUMN_SIGNED 0
#define embedDB_COLUMN_UNSIGNED 1
#define embedDB_IS_COL_SIGNED(colSize) (colSize < 0 ? 1 : 0)

/**
 * @brief	A struct to desribe the number and sizes of attributes contained in the data of a embedDB table
 */
typedef struct {
    uint8_t numCols;      // The number of columns in the table
    int8_t* columnSizes;  // A list of the sizes, in bytes, of each column. Negative numbers indicate signed columns while positive indicate an unsigned column
} embedDBSchema;

/**
 * @brief	Create an embedDBSchema from a list of column sizes including both key and data
 * @param	numCols			The total number of key & data columns in table
 * @param	colSizes		An array with the size of each column. Max size is 127
 * @param	colSignedness	An array describing if the data in the column is signed or unsigned. Use the defined constants embedDB_COLUMNN_SIGNED or embedDB_COLUMN_UNSIGNED
 */
embedDBSchema* embedDBCreateSchema(uint8_t numCols, int8_t* colSizes, int8_t* colSignedness);

/**
 * @brief	Free a schema. Sets the schema pointer to NULL.
 */
void embedDBFreeSchema(embedDBSchema** schema);

/**
 * @brief	Uses schema to determine the length of buffer to allocate and callocs that space
 */
void* createBufferFromSchema(embedDBSchema* schema);

/**
 * @brief	Deep copy schema and return a pointer to the copy
 */
embedDBSchema* copySchema(const embedDBSchema* schema);

/**
 * @brief	Finds byte offset of the column from the beginning of the record
 */
uint16_t getColOffsetFromSchema(embedDBSchema* schema, uint8_t colNum);

/**
 * @brief	Calculates record size from schema
 */
uint16_t getRecordSizeFromSchema(embedDBSchema* schema);

void printSchema(embedDBSchema* schema);

#endif
