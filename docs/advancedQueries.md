# Simple Query Interface

SBITS has a library with quick and easy-to-use query operators. To use them, import the [`advancedQueries.h`](../src/sbits/advancedQueries.h) header file. The builtin operators are not going to be the best solution in terms of performance because they are built to be highly compatible and simple to use. If performance is your priority consider a custom solution using the functions descibed in the [usage documentation](usageInfo.md).

For a complete code example see [advancedQueryExamples.c](../src/advancedQueryExamples.c), but this document is a guide on each provided operator as well as how to create custom operators.

## Table of Contents

## Builtin Operators

### Schema

The schema object is used to track the size, number ond positions of columns in an operator's input/output. Unlike in the base implementation of SBITS, where the key and data are always seperate, this library combines them into a single record pointer, and so the schema includes both the key and data columns.

To create an `sbitsSchema` object:

```c
// 4 columns, 4 bytes each
int8_t colSizes[] = {4, 4, 4, 4};

// The first column (key) is unsigned by requirement of SBITS, while the rest are signed values
int8_t colSignedness[] = {SBITS_COLUMN_UNSIGNED, SBITS_COLUMN_SIGNED, SBITS_COLUMN_SIGNED, SBITS_COLUMN_SIGNED};

// Create schema. Args: 4 coulmuns, their sizes, their signedness
sbitsSchema* baseSchema = sbitsCreateSchema(4, colSizes, colSignedness);
```

Freeing `sbitsSchema`:

```c
sbitsFreeSchema(&baseSchema);
```

### Using Operators

Set up a chain of operators where a table scan is at the bottom and you progressively apply operators on top of the last. Notice that `scanOp` doesn't have an operator in its construction, but the rest do. Then you only need to `init()` the top level operator which will recursively initialize all operators in the chain. During initialization, the intermediate schemas will be calculated and buffers will be allocated.

```c
sbitsOperator* scanOp = createTableScanOperator(stateUWA, &it, baseSchema);
int32_t selVal = 200;
sbitsOperator* selectOp = createSelectionOperator(scanOp, 3, SELECT_GTE, &selVal);
sbitsOperator* projOp = createProjectionOperator(selectOp, 3, projCols);
projOp->init(projOp);
```

After initialization, the top level operator can be executed so that the next record will end up in the `recordBuffer` of the top level operator. You can then read the data in this buffer/perform any other calulations if required.

```c
int32_t* recordBuffer = projOp->recordBuffer;
while(exec(projOp)) {
	printf("%-10lu | %-4.1f | %-4.1f\n", recordBuffer[0], recordBuffer[1] / 10.0, recordBuffer[2] / 10.0);
}
```

To free the operators, first `close()` the top level operator, which will recursively close the whole chain. Then you can either call `sbitsFreeOperatorRecursive()` to free the whole chain of operators, or, if any of the operators have two inputs (such as a join), you can call `free()` each operator individually.

```c
projOp->close(projOp);
// This
sbitsFreeOperatorRecursive(&projOp);
// Or this (required if any operator in chain has more than one input)
free(scanOp);
free(selectOp);
free(projOp);
```

### Table Scan

The table scan is the base of all queries. It uses an `sbitsIterator` to read records from the database.

```c
// Create and init iterator object with any constraints desired
sbitsIterator it;
int32_t maxTemp = 400;
it.minKey = NULL;
it.maxKey = NULL;
it.minData = NULL;
it.maxData = &maxTemp;
sbitsInitIterator(state, &it);

// Create schema object of the records returned by the operator
int8_t colSizes[] = {4, 4, 4, 4};
int8_t colSignedness[] = {SBITS_COLUMN_UNSIGNED, SBITS_COLUMN_SIGNED, SBITS_COLUMN_SIGNED, SBITS_COLUMN_SIGNED};
sbitsSchema* baseSchema = sbitsCreateSchema(4, colSizes, colSignedness);

// Create operator
sbitsOperator* scanOp = createTableScanOperator(state, &it, baseSchema);
```

### Projection

Projects out columns of the input operator. Projected columns must be in the order in which they are in the input operator. Column indexes are zero-indexed

```c
uint8_t projCols[] = {0, 1, 3}; // Must be strictly increasing. i.e. cannot have column 3 before column 1
sbitsOperator* projOp1 = createProjectionOperator(scanOp, 3, projCols);
```

### Selection

Performs a `SELECT * WHERE x` on the output of an operator. Supports >, >=, <, <=, ==, and != through the defined constants `SELECT_GT`, `SELECT_GTE`, etc.

The following selects tuples where column 3 (zero-indexed) is >= 200

```c
int32_t selVal = 200;
sbitsOperator* selectOp2 = createSelectionOperator(scanOp, 3, SELECT_GTE, &selVal);
```

### Aggregate Functions

This operator allows you to run a `GROUP BY` and perform an aggregate function on each group. In order to use this operator, you will need another type of object: `sbitsAggregateFunc`. The output of an aggregate operator is dictated by the list of `sbitsAggregateFunc` provided to `createAggregateOperator()`.

First, this is the code to setup the operator:

```c
sbitsAggregateFunc groupName = {NULL, NULL, writeDayGroup, NULL, 4};
sbitsAggregateFunc* counter = createCountAggregate();
sbitsAggregateFunc* sum = createSumAggregate(8, -4);
sbitsAggregateFunc aggFunctions[] = {groupName, *counter, *sum};
uint32_t numOps = 3;
sbitsOperator* aggOp3 = createAggregateOperator(selectOp3, sameDayGroup, aggFunctions, numOps);
```

But let's break it down. Both the counter and the sum are builtin aggregate functions created with thier respective functions. The sum function has two arguments: byte offset from the beginning of the
