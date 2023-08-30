# Running the Project

#### Dependencies

- GNU Make:
  - 4.4

## Building and Running the Project

This project can be built and run using the included [makefile](../makefile). There are two test files which can be used to run an example of EmbedDB: [embedDbExample](../examples/embedDBExample.c) and [embedDBVariableDataExample](../examples/embedDBVariableDataExample.c). The only difference is that the [embedDBVariableDataExample](../examples/embedDBVariableDataExample.c) file can be configured to run the project using the variable length data feature.

### EmbedDB Example

 To build and run the project with this file, run the command:

```
make embedDBExample
```

### EmbedDB Variable Data Example

To build and run the project with this file, run the command:

```
make embedDBVariableExample
```

## Removing Built Files

To remove any files built or generated using the above commands or the [test](/docs/testInfo.md) command, run the following command:

```
make clean
```
