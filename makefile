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

.PHONY: clean
.PHONY: test

PATHU = Unity/src/
PATHS = src/
PATHSBITS = src/sbits/
PATHSPLINE = src/spline/
PATH_QUERY = src/query-interface/

PATHT = test/
PATHB = build/
PATHD = build/depends/
PATHO = build/objs/
PATHR = build/results/
PATHA = build/artifacts/

BUILD_PATHS = $(PATHB) $(PATHD) $(PATHO) $(PATHR) $(PATHA)

SBITS_OBJECTS = $(PATHO)sbits.o $(PATHO)spline.o $(PATHO)radixspline.o $(PATHO)utilityFunctions.o 

QUERY_OBJECTS = $(PATHO)schema.o $(PATHO)advancedQueries.o

TEST_FLAGS = -I. -I $(PATHU) -I $(PATHS) -D TEST

COMMON_FLAGS = -I. -I$(PATHS) -D PRINT_ERRORS

CFLAGS = $(if $(filter test,$(MAKECMDGOALS)),$(TEST_FLAGS),$(COMMON_FLAGS))

SRCT = $(wildcard $(PATHT)*.c)

VARTEST = $(PATHO)varTest.o
TEST_SBITS = $(PATHO)testSbits.o
ADVANCED_QUERY = $(PATHO)advancedQueryExamples.o

COMPILE=gcc -c
LINK=gcc
DEPEND=gcc -MM -MG -MF

RESULTS = $(patsubst $(PATHT)Test%.c,$(PATHR)Test%.testpass,$(SRCT))

varTest: $(BUILD_PATHS) $(PATHB)varTest.$(TARGET_EXTENSION)
	@echo "Running varTest"
	-./$(PATHB)varTest.$(TARGET_EXTENSION)
	@echo "Finished running varTest file"

$(PATHB)varTest.$(TARGET_EXTENSION): $(SBITS_OBJECTS) $(VARTEST)
	$(LINK) -o $@ $^ $(MATH)

testSbits: $(BUILD_PATHS) $(PATHB)testSbits.$(TARGET_EXTENSION)
	@echo "Running testSbits"
	-./$(PATHB)testSbits.$(TARGET_EXTENSION)
	@echo "Finished running testSbits file"

$(PATHB)testSbits.$(TARGET_EXTENSION): $(SBITS_OBJECTS) $(TEST_SBITS)
	$(LINK) -o $@ $^

advancedQueryExamples: $(BUILD_PATHS) $(PATHB)advancedQueryExamples.$(TARGET_EXTENSION)
	-./$(PATHB)advancedQueryExamples.$(TARGET_EXTENSION)

$(PATHB)advancedQueryExamples.$(TARGET_EXTENSION): $(SBITS_OBJECTS) $(QUERY_OBJECTS) $(ADVANCED_QUERY)
	$(LINK) -o $@ $^

test: $(BUILD_PATHS) $(RESULTS)
	pip install -r requirements.txt -q
	$(PYTHON) ./scripts/stylize_as_junit.py

$(PATHR)%.testpass: $(PATHB)%.$(TARGET_EXTENSION)
	-./$< > $@ 2>&1

$(PATHB)Test%.$(TARGET_EXTENSION): $(PATHO)Test%.o $(SBITS_OBJECTS) $(PATHO)unity.o #$(PATHD)Test%.d
	$(LINK) -o $@ $^ $(MATH)

$(PATHO)%.o:: $(PATHT)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHS)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHSPLINE)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHSBITS)%.c
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
