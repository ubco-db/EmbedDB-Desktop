# Running the Project

#### Dependencies

- GNU Make:
  - 4.4

## Building and Running the Project

This project can be built and run using the included [makefile](../makefile). There are two test files which can be used to run an example of SBITS: [test_sbits](../src/test_sbits.c) and [varTest](../src/varTest.c). The only difference is that the varTest file can be configured to run the project using the variable length data feature.

### test_sbits

 To build and run the project with this file, run the command:

```
make test_sbits
```

### varTest

To build and run the project with this file, run the command:

```
make varTest
```

## Removing Built Files

To remove any files built or generated using the above commands or the [test](/docs/testInfo.md) command, run the following command:

```
make clean
```
