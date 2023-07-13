ifeq ($(OS),Windows_NT)
  ifeq ($(shell uname -s),) # not in a bash-like shell
	CLEANUP = del /F /Q
	MKDIR = mkdir
  else # in a bash-like shell, like msys
	CLEANUP = rm -f
	MKDIR = mkdir -p
  endif
	TARGET_EXTENSION=exe
else
	CLEANUP = rm -f
	MKDIR = mkdir -p
	TARGET_EXTENSION=out
endif

.PHONY: clean
.PHONY: test

PATHU = Unity/src/
PATHS = src/
PATHSBITS = src/sbits/
PATHSPLINE = src/spline/
PATHT = test/
PATHB = build/
PATHD = build/depends/
PATHO = build/objs/
PATHR = build/results/
PATHA = build/artifacts/

BUILD_PATHS = $(PATHB) $(PATHD) $(PATHO) $(PATHR) $(PATHA)

OBJECTS = $(PATHO)sbits.o $(PATHO)spline.o $(PATHO)radixspline.o $(PATHO)utilityFunctions.o

SRCT = $(wildcard $(PATHT)*.c)

VARTEST = $(PATHO)varTest.o
TEST_SBITS = $(PATHO)test_sbits.o

COMPILE=gcc -c
LINK=gcc
DEPEND=gcc -MM -MG -MF
CFLAGS=-I. -I$(PATHU) -I$(PATHS) -DTEST -lm

RESULTS = $(patsubst $(PATHT)Test%.c,$(PATHR)Test%.testpass,$(SRCT))

varTest: $(BUILD_PATHS) $(PATHB)varTest.$(TARGET_EXTENSION)
	@echo "Running varTest"
	-./$(PATHB)varTest.$(TARGET_EXTENSION)
	@echo "Finished running varTest file"

$(PATHB)varTest.$(TARGET_EXTENSION): $(OBJECTS) $(VARTEST)
	$(LINK) -o $@ $^ -lm

test_sbits: $(BUILD_PATHS) $(PATHB)test_sbits.$(TARGET_EXTENSION)
	@echo "Running test_sbits"
	-./$(PATHB)test_sbits.$(TARGET_EXTENSION)
	@echo "Finished running test_sbits file"

$(PATHB)test_sbits.$(TARGET_EXTENSION): $(OBJECTS) $(TEST_SBITS)
	$(LINK) -o $@ $^ -lm

test: $(BUILD_PATHS) $(RESULTS)
	pip install -r requirements.txt -q
	python ./scripts/stylize_as_junit.py

$(PATHR)%.testpass: $(PATHB)%.$(TARGET_EXTENSION)
	-./$< > $@ 2>&1 -lm

$(PATHB)Test%.$(TARGET_EXTENSION): $(PATHO)Test%.o $(OBJECTS) $(PATHO)unity.o #$(PATHD)Test%.d
	$(LINK) -o $@ $^ -lm

$(PATHO)%.o:: $(PATHT)%.c
	$(COMPILE) $(CFLAGS) $< -o $@ -lm

$(PATHO)%.o:: $(PATHS)%.c
	$(COMPILE) $(CFLAGS) $< -o $@ -lm

$(PATHO)%.o:: $(PATHSPLINE)%.c
	$(COMPILE) $(CFLAGS) $< -o $@ -lm

$(PATHO)%.o:: $(PATHSBITS)%.c
	$(COMPILE) $(CFLAGS) $< -o $@ -lm

$(PATHO)%.o:: $(PATHU)%.c $(PATHU)%.h
	$(COMPILE) $(CFLAGS) $< -o $@ -lm

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
