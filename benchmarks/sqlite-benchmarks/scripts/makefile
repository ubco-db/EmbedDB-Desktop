ifeq ($(OS),Windows_NT)
  ifeq ($(shell uname -s),) # not in a bash-like shell
	CLEANUP = del /F /Q
	MKDIR = mkdir
  else # in a bash-like shell, like msys
	CLEANUP = rm -f
	MKDIR = mkdir -p
  endif
	TARGET_EXTENSION=exe
	OSFLAGS =
	SQLITEOSFLAG = 
else
	CLEANUP = rm -f
	MKDIR = mkdir -p
	TARGET_EXTENSION=out
	OSFLAGS = -ldl 
	SQLITEOSFLAG = -lpthread
endif

.PHONY: clean

PATHSBITS = ../../../src/sbits/
PATHSPLINE = ../../../src/spline/

sqliteBenchmarking:
	gcc -c sqlite3.c sqliteBenchmarking.c $(OSFLAGS) $(SQLITEOSFLAG)
	gcc -o sqliteBenchmarking.$(TARGET_EXTENSION) sqlite3.o sqliteBenchmarking.o $(OSFLAGS) $(SQLITEOSFLAG)
	./sqliteBenchmarking.$(TARGET_EXTENSION)

sbitsBenchmarking:
	gcc -c $(PATHSBITS)sbits.c -o sbits.o $(OSFLAGS)
	gcc -c $(PATHSBITS)utilityFunctions.c -o utilityFunctions.o  $(OSFLAGS)
	gcc -c $(PATHSPLINE)spline.c -o spline.o $(OSFLAGS)
	gcc -c $(PATHSPLINE)radixspline.c -o radixspline.o $(OSFLAGS)
	gcc -c sbitsBenchmarking.c -o sbitsBenchmarking.o $(OSFLAGS)
	gcc -o sbitsBenchmarking.$(TARGET_EXTENSION) sbitsBenchmarking.o sbits.o utilityFunctions.o spline.o radixspline.o $(OSFLAGS)
	./sbitsBenchmarking.$(TARGET_EXTENSION)

clean:
	$(CLEANUP) *.o	
	$(CLEANUP) *.$(TARGET_EXTENSION)
	$(CLEANUP) *.db
	$(CLEANUP) *.bin
