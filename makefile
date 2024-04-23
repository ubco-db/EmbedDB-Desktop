ifeq ($(OS),Windows_NT)
  ifeq ($(shell uname -s),) # not in a bash-like shell
    CLEANUP = del /F /Q
    MKDIR = mkdir
  else # in a bash-like shell, like msys
    CLEANUP = rm -f
    MKDIR = mkdir -p
  endif
    MATH=
    PYTHON=python
    TARGET_EXTENSION=exe
else
  MATH = -lm
  CLEANUP = rm -f
  MKDIR = mkdir -p
  TARGET_EXTENSION=out
  PYTHON=python3
endif

.PHONY: clean test examples timestampSequentialExample

PATHU = Unity/src/
PATHS = src/
PATHBENCH = benchmarks/
PATH_EMBEDDB = src/embedDB/
PATHSPLINE = src/spline/
PATH_QUERY = src/query-interface/
PATHEXAMPLES = examples/

PATHT = test/
PATHB = build/
PATHD = build/depends/
PATHO = build/objs/
PATHR = build/results/
PATHA = build/artifacts/

BUILD_PATHS = $(PATHB) $(PATHD) $(PATHO) $(PATHR) $(PATHA)

EMBEDDB_OBJECTS = $(PATHO)embedDB.o $(PATHO)spline.o $(PATHO)radixspline.o $(PATHO)utilityFunctions.o 
QUERY_OBJECTS = $(PATHO)schema.o $(PATHO)advancedQueries.o

TEST_FLAGS = -I. -I $(PATHU) -I $(PATHS) -D TEST
EXAMPLE_FLAGS = -I. -I$(PATHS) -I$(PATHBENCH) -D PRINT_ERRORS

CFLAGS = $(if $(filter test,$(MAKECMDGOALS)),$(TEST_FLAGS),$(EXAMPLE_FLAGS))

SRCT = $(wildcard $(PATHT)*.c)
EXAMPLES_SRC = $(wildcard $(PATHEXAMPLES)*.c)
EXAMPLES_OBJECTS = $(patsubst $(PATHEXAMPLES)%.c,$(PATHO)%.o,$(EXAMPLES_SRC))

EMBED_VARIABLE_BENCHMARK = $(PATHO)embedDBVariableBenchmark.o
EMBEDDB_BENCHMARK = $(PATHO)embedDBBenchmark.o
QUERY_INTERFACE_BENCHMARK = $(PATHO)advancedQueryInterfaceBenchmark.o
TIMESTAMP_SEQUENTIAL_EXAMPLE = $(PATHO)timestampSequentialExample.o

COMPILE=gcc -c
LINK=gcc
DEPEND=gcc -MM -MG -MF

RESULTS = $(patsubst $(PATHT)Test%.c,$(PATHR)Test%.testpass,$(SRCT))

$(PATHO)%.o:: $(PATHEXAMPLES)%.c
	$(COMPILE) $(EXAMPLE_FLAGS) $< -o $@

$(PATHB)%.$(TARGET_EXTENSION): $(PATHO)%.o $(EMBEDDB_OBJECTS) $(QUERY_OBJECTS)
	$(LINK) -o $@ $^ $(MATH)

embedDBExample: $(PATHB)embedDBExample.$(TARGET_EXTENSION)
	-./$(PATHB)embedDBExample.$(TARGET_EXTENSION)

timestampSequentialExample: $(PATHB)timestampSequentialExample.$(TARGET_EXTENSION)
	-./$(PATHB)timestampSequentialExample.$(TARGET_EXTENSION)

embedDBVariableBenchmark: $(BUILD_PATHS) $(PATHB)embedDBVariableBenchmark.$(TARGET_EXTENSION)
	@echo "Running EmbedDB variable data benchmark"
	-./$(PATHB)embedDBVariableBenchmark.$(TARGET_EXTENSION)
	@echo "Finished running EmbedDB variable data benchmark"

$(PATHB)embedDBVariableBenchmark.$(TARGET_EXTENSION): $(EMBEDDB_OBJECTS) $(EMBED_VARIABLE_BENCHMARK)
	$(LINK) -o $@ $^ $(MATH)

embedDBBenchmark: $(BUILD_PATHS) $(PATHB)embedDBBenchmark.$(TARGET_EXTENSION)
	@echo "Running EmbedDB Benchmark"
	-./$(PATHB)embedDBBenchmark.$(TARGET_EXTENSION)
	@echo "Finished running EmbedDB benchmark"

$(PATHB)embedDBBenchmark.$(TARGET_EXTENSION): $(EMBEDDB_OBJECTS) $(EMBEDDB_BENCHMARK)
	$(LINK) -o $@ $^ $(MATH)

queryBenchmark: $(BUILD_PATHS) $(PATHB)advancedQueryInterfaceBenchmark.$(TARGET_EXTENSION)
	@echo "Running Advanced Query Interface Benchmark"
	-./$(PATHB)advancedQueryInterfaceBenchmark.$(TARGET_EXTENSION)
	@echo "Finished running Advanced Query Interface Benchmark"

$(PATHB)advancedQueryInterfaceBenchmark.$(TARGET_EXTENSION): $(EMBEDDB_OBJECTS) $(QUERY_OBJECTS) $(QUERY_INTERFACE_BENCHMARK)
	$(LINK) -o $@ $^ $(MATH)

test: $(BUILD_PATHS) $(RESULTS)
	pip install -r requirements.txt -q
	$(PYTHON) ./scripts/stylize_as_junit.py

$(PATHR)%.testpass: $(PATHB)%.$(TARGET_EXTENSION)
	-./$< > $@ 2>&1

$(PATHB)Test%.$(TARGET_EXTENSION): $(PATHO)Test%.o $(EMBEDDB_OBJECTS) $(QUERY_OBJECTS) $(PATHO)unity.o
	$(LINK) -o $@ $^ $(MATH)

$(PATHO)%.o:: $(PATHT)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHBENCH)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHSPLINE)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATH_EMBEDDB)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATH_QUERY)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHU)%.c $(PATHU)%.h
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHD)%.d:: $(PATHT)%.c
	$(DEPEND) $@ $<

$(PATHB):
	$(MKDIR) $(PATHB)

$(PATHD):
	$(MKDIR) $(PATHD)

$(PATHO):
	$(MKDIR) $(PATHO)

$(PATHR):
	$(MKDIR) $(PATHR)

$(PATHA):
	$(MKDIR) $(PATHA)

clean:
	$(CLEANUP) $(PATHO)*.o
	$(CLEANUP) $(PATHB)*.$(TARGET_EXTENSION)
	$(CLEANUP) $(PATHR)*.testpass
	$(CLEANUP) $(PATHA)*.png
	$(CLEANUP) $(PATHA)*.bin
	$(CLEANUP) $(PATHR)*.xml

.PRECIOUS: $(PATHB)Test%.$(TARGET_EXTENSION)
.PRECIOUS: $(PATHD)%.d
.PRECIOUS: $(PATHO)%.o
.PRECIOUS: $(PATHR)%.testpass