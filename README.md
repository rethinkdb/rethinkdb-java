# RethinkDB Java Driver

[ ![Download](https://api.bintray.com/packages/rethinkdb/maven/rethinkdb-driver/images/download.svg) ](https://bintray.com/rethinkdb/maven/rethinkdb-driver/_latestVersion)

This is the official [RethinkDB](https://rethinkdb.com/) client driver for Java and other JVM languages.

The driver has official docs that you can read at [the RethinkDB documentation](http://rethinkdb.com/api/java/).

## Building from source

To build from source you just need JDK 8 or greater.

Run `./gradlew assemble` to build the jar or `./gradlew install` to install it into your local maven repository.

## Contributing to the driver

### Installation

Besides JDK 8, to be able to contribute to the driver, you must also install:

* Python *3.6* or *3.7*
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

[*] Currently, `genMainJava` task is disabled because the `generatedJsonFiles` python scripts are generating invalid JSON files.

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

To deploy, you'll need to create a file called `confidential.properties` in the same directory as `build.gradle.kts` 
(Alternatively, you can do the same`gradle.properties` at `~/.gradle` (`%USERPROFILE%\.gradle` on Windows) with the following:

```
signing.keyId=<KEY_ID>
signing.password=
signing.secretKeyRingFile=<KEYRING_LOCATION>

ossrhUsername=<SONATYPE_USERNAME>
ossrhPassword=<SONATYPE_PASSWORD>
```

You should note that there's a `gradle.properties` in this repository, but you shouldn't add the above into it,
otherwise your credentials can be checked back into git. Create the `confidential.properties` file, which is added into
`.gitignore`, or create the file in the `.gradle` folder, in order to prevent accidents.

You'll need to add your gpg signing key id and keyring file. Usually, the keyring file is located at`~/.gnupg/secring.gpg`,
but Gradle won't expand home-dirs in the config file so you have to put the absolute path to your keyring file.
If you don't have a password on your private key for package signing, leave the `signing.password=` line **empty**.

You must use gpg 2.0 or below, since gpg 2.1 and greater doesn't use the `secring.gpg` file anymore. This is a limitation
on Gradle's end and there's an [issue regarding that](https://github.com/gradle/gradle/issues/888).

You also neeed a Sonatype username and password, that you may get from [Sonatype's JIRA](https://issues.sonatype.org/secure/Signup!default.jspa),
with access to the `com.rethinkdb` group. Some RethinkDB maintainer may already have it.

To upload a new release, run the Gradle task `uploadArchives`. This should sign and upload the package to the release
repository. This is for official releases/betas etc. If you just want to upload a snapshot, add the suffix `-SNAPSHOT`
to the `version` value in `build.gradle`. The gradle maven plugin decides which repo to upload to depending on whether
the version looks like `2.2` or `2.2-SNAPSHOT`, so this is important to get right or it won't go to the right place.

If you just want to do a snapshot: if `gradle uploadArchives` succeeds, you're done. The snapshot will be located at
https://oss.sonatype.org/content/repositories/snapshots/com/rethinkdb/rethinkdb-driver/ with the version you gave it.

If you are doing a full release, you need to go to https://oss.sonatype.org/#stagingRepositories and search for
"rethinkdb" in the search box, find the release that is in status `open`. Select it and then click the `Close` button.
This will check it and make it ready for release. If that stage passes you can click the `Release` button.

For full instructions see: http://central.sonatype.org/pages/releasing-the-deployment.html
