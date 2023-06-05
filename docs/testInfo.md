# Test Information

This project is tested using the [Unity](https://github.com/ThrowTheSwitch/Unity) C testing library. The source code for the library is located in the lib folder.

## Running the Tests

#### Dependencies

- pip:
  - 23.0

#### Executing the tests

To run the tests, you must run the [makefile](../makefile) using the command `make`. Depending on your operating system, you may have to install make, and the command may be different.

## Information About Testing Setup

The tests are run using the [makefile](../makefile). This runs the tests by searching through the [src](../src/) and [test](../test/) folders, finding matching files, building the source code and tests, and runs the generated executable file.

- Note that becuase of how the tests are found, they must be named using the convention where if the srouce file is named `Math.c`, the test file must be named `TestMath.c`.

Unity records the sucess and failures of the test suites in a text file in the [results](../build/results/) folder. Afterwards, all of the files are parsed using a python script to build a JUnit XML format file to summarize the results.

- Both the [makefile](../makefile) and [JUnit Script](../scripts/stylize_as_junit.py) are sourced from the [Unity](https://github.com/ThrowTheSwitch/Unity) testing library. Minor modifications were made so they would work with this project. This includes changing directory locations and adding additional checks when parsing the output files in the python script. The only changes to the makefile are due to some c files being dependent on multiple other files.
