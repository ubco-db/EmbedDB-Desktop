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
PATHQUERYIFACE = ../../src/query-interface/

converterBenchmark:
	gcc -c $(PATHEMBEDDB)embedDB.c -o embedDB.o $(OSFLAGS)
	gcc -c $(PATHEMBEDDB)utilityFunctions.c -o utilityFunctions.o  $(OSFLAGS)
	gcc -c $(PATHSPLINE)spline.c -o spline.o $(OSFLAGS)
	gcc -c $(PATHSPLINE)radixspline.c -o radixspline.o $(OSFLAGS)
	gcc -c $(PATHQUERYIFACE)schema.c -o schema.o $(OSFLAGS)
	gcc -c $(PATHQUERYIFACE)advancedQueries.c -o advancedQueries.o $(OSFLAGS)
	gcc -c query1.c -o query1.o $(OSFLAGS)
	gcc -c query2.c -o query2.o $(OSFLAGS)
	gcc -c query3.c -o query3.o $(OSFLAGS)
	gcc -c customFunctions.c -o customFunctions.o $(OSFLAGS)
	gcc -c converterBenchmark.c -o converterBenchmark.o $(OSFLAGS)
	gcc -o converterBenchmark.$(TARGET_EXTENSION) converterBenchmark.o embedDB.o utilityFunctions.o spline.o radixspline.o schema.o advancedQueries.o query1.o query2.o query3.o customFunctions.o $(OSFLAGS)
	./converterBenchmark.$(TARGET_EXTENSION)

clean:
	$(CLEANUP) *.o	
	$(CLEANUP) *.$(TARGET_EXTENSION)
	$(CLEANUP) *.db
	$(CLEANUP) *.bin
