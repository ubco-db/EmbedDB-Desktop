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

embedDBBenchmarkEthylene4M:
	gcc embedDBBenchmarkEthylene4M.c ../src/embedDB/embedDB.c ../src/embedDB/utilityFunctions.c ../src/spline/spline.c ../src/spline/radixspline.c -o embedDBBenchmarkEthylene4M.$(TARGET_EXTENSION) $(OSFLAGS)
	./embedDBBenchmarkEthylene4M.$(TARGET_EXTENSION)

clean:
	$(CLEANUP) *.o	
	$(CLEANUP) *.$(TARGET_EXTENSION)
	$(CLEANUP) *.db
	$(CLEANUP) *.bin
