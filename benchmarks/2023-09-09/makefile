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
	OSFLAGS = -ldl -lm
	SQLITEOSFLAG = -lpthread
endif

.PHONY: clean

PATHEMBEDDB = ../../src/embedDB/
PATHSPLINE = ../../src/spline/

sqliteBenchmark:
	gcc -c sqlite3.c sqliteBenchmark.c $(OSFLAGS) $(SQLITEOSFLAG)
	gcc -o sqliteBenchmark.$(TARGET_EXTENSION) sqlite3.o sqliteBenchmark.o $(OSFLAGS) $(SQLITEOSFLAG)
	./sqliteBenchmark.$(TARGET_EXTENSION)

embedDBBenchmark:
	gcc -c $(PATHEMBEDDB)embedDB.c -o embedDB.o $(OSFLAGS)
	gcc -c $(PATHEMBEDDB)utilityFunctions.c -o utilityFunctions.o  $(OSFLAGS)
	gcc -c $(PATHSPLINE)spline.c -o spline.o $(OSFLAGS)
	gcc -c $(PATHSPLINE)radixspline.c -o radixspline.o $(OSFLAGS)
	gcc -c embedDBBenchmark.c -o embedDBBenchmark.o $(OSFLAGS)
	gcc -o embedDBBenchmark.$(TARGET_EXTENSION) embedDBBenchmark.o embedDB.o utilityFunctions.o spline.o radixspline.o $(OSFLAGS)
	./embedDBBenchmark.$(TARGET_EXTENSION)

clean:
	$(CLEANUP) *.o	
	$(CLEANUP) *.$(TARGET_EXTENSION)
	$(CLEANUP) *.bin
