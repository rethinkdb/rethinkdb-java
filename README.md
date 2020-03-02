# RethinkDB Java Driver

[![Maven Central](https://img.shields.io/maven-central/v/com.rethinkdb/rethinkdb-driver)](https://search.maven.org/artifact/com.rethinkdb/rethinkdb-driver)
[![Bintray](https://img.shields.io/bintray/v/rethinkdb/maven/rethinkdb-driver)](https://bintray.com/rethinkdb/maven/rethinkdb-driver/_latestVersion)
[![Travis-CI.org](https://img.shields.io/travis/rethinkdb/rethinkdb-java)](https://travis-ci.org/rethinkdb/rethinkdb-java)
[![Twitter](https://img.shields.io/twitter/url?style=social&url=https%3A%2F%2Fgithub.com%2Frethinkdb%2Frethinkdb-java)](https://twitter.com/intent/tweet?text=Wow:&url=https%3A%2F%2Fgithub.com%2Frethinkdb%2Frethinkdb-java)

This is the official [RethinkDB](https://rethinkdb.com/) client driver for Java and other JVM languages.

The driver has official docs that you can read at [the RethinkDB documentation](http://rethinkdb.com/api/java/).

## What changed in Version 2.4.1?

This version is **full of breaking changes**, and is equivalent to a **major release**, but being compatible with RethinkDB v2.4.0

Please read the [release notes](https://github.com/rethinkdb/rethinkdb-java/releases) for v2.4.1 to know what changed.

## Building from source

To build from source you just need JDK 8 or greater.

Run `./gradlew assemble` to build the jar or `./gradlew install` to install it into your local maven repository.

## Contributing to the driver

### Installation

Besides JDK 8, to be able to contribute to the driver, you must also install:

* Python **3.6** or **3.7**
* PIP3 libraries:
  * mako
  * rethinkdb

### Using Gradle

Gradle has some special tasks that help you managing the driver, such as downloading sources from the main repository.

You can invoke any task by running `./gradlew taskName` (or `gradlew.bat` in Windows) and get a list of tasks by running `./gradlew tasks`

| Task                    | Description                                                                                                       |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `downloadProtoAndTests` | Downloads the proto file and the YAML test files into the repository.                                             |
| `generateJsonFiles`     | Using the downloaded proto file, rgenerates the required JSON files for java file generation.                     |
| `genMainJava`*          | Using all the JSON files, regenerates the files on the `com.rethinkdb.gen` package at `src/main/java`             |
| `genTestJava`           | Using the downloaded YAML test files, regenerates the tests on the `com.rethinkdb.gen` package at `src/test/java` |

[*] Currently, `genMainJava` task is enabled only for local files because the `generatedJsonFiles` python scripts are generating invalid JSON files.

To setup your environiment to be able to generate driver classes and test files, run the `downloadProtoAndTests` and `generateJsonFiles` tasks.

### Basics

The build process runs a python script (`metajava.py`) that
automatically generates the required Java classes for reql terms. The
process looks like this:

```
metajava.py creates:

ql2.proto -> proto_basic.json ---+
        |                        |
        |                        v
        +--> term_info.json -> build/java_term_info.json
                                 |
                                 v
global_info.json ------------> templates/ -> src/main/java/com/rethinkdb/gen/ast/{Term}.java
                                       |
                                       +---> src/main/java/com/rethinkdb/gen/proto/{ProtocolType}.java
                                       +---> src/main/java/com/rethinkdb/gen/model/TopLevel.java
                                       +---> src/main/java/com/rethinkdb/gen/exc/Reql{Exception}Error.java
```

Generally, you won't need to use metajava.py if you only want to build
the current driver, since all generated files are checked into
git. Only if you want to modify the templates in `templates/` will you
need python3, rethinkdb (python package) and mako (python package)
installed.

If you're building the java driver without changing the template
files, you can simply do:

```bash
$ ./gradlew assemble
# or if you want to run tests as well
$ ./gradlew build
```

This will create the .jar files in `build/libs`.

### Testing

Tests are created from the polyglot yaml tests located in the rethinkdb github repository's `test/rql_test/src/` directory.
To get the polyglot yaml tests, run the Gradle task `downloadProtoAndTests`. Gradle will checkout the tests into `build/rethinkdb_gen`.

The `genTestJava` task is used to run the test conversion, which requires python3 and the above listed python dependencies.
It will output JUnit test files into `src/test/java/com/rethinkdb/gen`, using `process_polyglot.py` and `convert_tests.py`.
These are also checked into git, so you don't need to run the conversion script if you just want to verify that the existing tests pass.

`process_polyglot.py` is intended to be independent of the java driver, and only handles reading in the polyglot tests and normalizing them into a format that's easier to use to generate new tests from.

**TL;DR**: `process_polyglot.py` doesn't have the word "java" anywhere in it, while `convert_tests.py` has all of the java specific behavior in it and builds on top of the stream of test definitions that `process_polyglot.py` emits.

## Deploying a release or snapshot

This section was moved to separate documentation:

> [How to deploy this repository to Bintray](DEPLOYING-BINTRAY.md) 

> [How to deploy this repository to Maven Central (Sonatype)](DEPLOYING-SONATYPE.md)