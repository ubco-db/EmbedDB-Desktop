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

sqliteBenchmarkUWA:
	gcc -c sqlite3.c sqliteBenchmarkUWA.c $(OSFLAGS) $(SQLITEOSFLAG)
	gcc -o sqliteBenchmarkUWA.$(TARGET_EXTENSION) sqlite3.o sqliteBenchmarkUWA.o $(OSFLAGS) $(SQLITEOSFLAG)
	./sqliteBenchmarkUWA.$(TARGET_EXTENSION)

sqliteBenchmarkWatch:
	gcc -c sqlite3.c sqliteBenchmarkWatch.c $(OSFLAGS) $(SQLITEOSFLAG)
	gcc -o sqliteBenchmarkWatch.$(TARGET_EXTENSION) sqlite3.o sqliteBenchmarkWatch.o $(OSFLAGS) $(SQLITEOSFLAG)
	./sqliteBenchmarkWatch.$(TARGET_EXTENSION)

embedDBBenchmarkUWA:
	gcc -c $(PATHEMBEDDB)embedDB.c -o embedDB.o $(OSFLAGS)
	gcc -c $(PATHEMBEDDB)utilityFunctions.c -o utilityFunctions.o  $(OSFLAGS)
	gcc -c $(PATHSPLINE)spline.c -o spline.o $(OSFLAGS)
	gcc -c $(PATHSPLINE)radixspline.c -o radixspline.o $(OSFLAGS)
	gcc -c embedDBBenchmarkUWA.c -o embedDBBenchmarkUWA.o $(OSFLAGS)
	gcc -o embedDBBenchmarkUWA.$(TARGET_EXTENSION) embedDBBenchmarkUWA.o embedDB.o utilityFunctions.o spline.o radixspline.o $(OSFLAGS)
	./embedDBBenchmarkUWA.$(TARGET_EXTENSION)

embedDBBenchmarkWatch:
	gcc -c $(PATHEMBEDDB)embedDB.c -o embedDB.o $(OSFLAGS)
	gcc -c $(PATHEMBEDDB)utilityFunctions.c -o utilityFunctions.o  $(OSFLAGS)
	gcc -c $(PATHSPLINE)spline.c -o spline.o $(OSFLAGS)
	gcc -c $(PATHSPLINE)radixspline.c -o radixspline.o $(OSFLAGS)
	gcc -c embedDBBenchmarkWatch.c -o embedDBBenchmarkWatch.o $(OSFLAGS)
	gcc -o embedDBBenchmarkWatch.$(TARGET_EXTENSION) embedDBBenchmarkWatch.o embedDB.o utilityFunctions.o spline.o radixspline.o $(OSFLAGS)
	./embedDBBenchmarkWatch.$(TARGET_EXTENSION)

clean:
	$(CLEANUP) *.o	
	$(CLEANUP) *.$(TARGET_EXTENSION)
	$(CLEANUP) *.bin
