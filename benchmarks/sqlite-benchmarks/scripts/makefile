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

PATHSBITS = ../../../src/sbits/
PATHSPLINE = ../../../src/spline/

sqliteBenchmarking:
	gcc -c sqlite3.c sqliteBenchmarking.c -ldl -lpthread
	gcc -o sqliteBenchmarking.$(TARGET_EXTENSION) sqlite3.o sqliteBenchmarking.o -ldl -lpthread
	sqliteBenchmarking.$(TARGET_EXTENSION)

sbitsBenchmarking:
	gcc -c $(PATHSBITS)sbits.c -o sbits.o 
	gcc -c $(PATHSBITS)utilityFunctions.c -o utilityFunctions.o 
	gcc -c $(PATHSPLINE)spline.c -o spline.o
	gcc -c $(PATHSPLINE)radixspline.c -o radixspline.o
	gcc -c sbitsBenchmarking.c -o sbitsBenchmarking.o
	gcc -o sbitsBenchmarking.exe sbitsBenchmarking.o sbits.o utilityFunctions.o spline.o radixspline.o
	sbitsBenchmarking.exe

clean:
	$(CLEANUP) *.o	
	$(CLEANUP) *.$(TARGET_EXTENSION)
	$(CLEANUP) *.db
	$(CLEANUP) *.bin
