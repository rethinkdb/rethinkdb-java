plugins {
    java
    maven
    signing
}

version = "2.4.0"
group = "com.rethinkdb"

val isReleaseVersion = !version.toString().endsWith("-SNAPSHOT")

java.sourceCompatibility = JavaVersion.VERSION_1_8
java.targetCompatibility = JavaVersion.VERSION_1_8

repositories {
    mavenCentral()
}

dependencies {
    testCompile("junit:junit:4.12")
    testCompile("net.jodah:concurrentunit:0.4.2")
    testRuntime("ch.qos.logback:logback-classic:1.1.3")
    compile("org.slf4j:slf4j-api:1.7.12")
    compile("com.googlecode.json-simple:json-simple:1.1.1")
    compile("com.fasterxml.jackson.core:jackson-databind:2.0.1")
}

signing {
    // Don't sign unless this is a release version
    sign(configurations.archives.get())
    gradle.taskGraph.whenReady(Action {
        isRequired = isReleaseVersion && hasTask("uploadArchives")
    })
}



fun findProperty(s: String) = project.findProperty(s) as String?
tasks {
    withType<JavaCompile> {
        options.encoding = "UTF-8"
    }
    val sourcesJar by creating(Jar::class) {
        group = "build"
        description = "Generates a jar with the sources"
        dependsOn(JavaPlugin.CLASSES_TASK_NAME)
        archiveClassifier.set("sources")
        from(sourceSets["main"].allSource)
    }

    val javadocJar by creating(Jar::class) {
        group = "build"
        description = "Generates a jar with the javadoc"
        dependsOn(JavaPlugin.JAVADOC_TASK_NAME)
        archiveClassifier.set("javadoc")
        from(javadoc)
    }

    artifacts {
        add("archives", sourcesJar)
        add("archives", javadocJar)
    }

    getByName<Upload>("uploadArchives") {
        repositories {
            withConvention(MavenRepositoryHandlerConvention::class) {
                mavenDeployer {
                    beforeDeployment(Action { signing.signPom(this) })

                    withGroovyBuilder {
                        "repository"("url" to uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")) {
                            "authentication"(
                                "userName" to findProperty("ossrhUsername"),
                                "password" to findProperty("ossrhPassword")
                            )
                        }
                        "snapshotRepository"("url" to uri("https://oss.sonatype.org/content/repositories/snapshots/")) {
                            "authentication"(
                                "userName" to findProperty("ossrhUsername"),
                                "password" to findProperty("ossrhPassword")
                            )
                        }
                    }

                    pom.project {
                        withGroovyBuilder {
                            "name"("RethinkDB Java Driver")
                            "packaging"("jar")
                            "description"("Official java driver for RethinkDB")
                            "url"("http://rethinkdb.com")

                            "scm" {
                                "connection"("scm:git:https://github.com/rethinkdb/rethinkdb-java")
                                "developerConnection"("scm:git:https://github.com/rethinkdb/rethinkdb-java")
                                "url"("https://github.com/rethinkdb/rethinkdb-java")
                            }

                            "licenses" {
                                "license" {
                                    "name"("The Apache License, Version 2.0")
                                    "url"("http://www.apache.org/licenses/LICENSE-2.0.txt")
                                }
                            }

                            "developers" {
                                "developer" {
                                    "id"("josh-rethinkdb")
                                    "name"("Josh Kuhn")
                                    "email"("josh@rethinkdb.com")
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    val downloadProtoAndTests by creating {
        group = "build setup"
        description = "Downloads contents from rethinkdb main repository."

        //properties
        val rethinkdb_repo = findProperty("build.rethinkdb_repo")
        val rethinkdb_branch = findProperty("build.rethinkdb_branch")
        val checkout_dir = findProperty("build.rethinkdb_checkout_dir")
        val proto_location = findProperty("build.proto.src_location")
        val proto_target = findProperty("build.proto.target_folder")
        val tests_location = findProperty("build.tests.src_location")
        val tests_target = findProperty("build.tests.target_folder")

        val proto_folder = File(buildDir, "rethinkdb_gen/$proto_target")
        val tests_folder = File(buildDir, "rethinkdb_gen/$tests_target")

        doLast {
            File(buildDir, "rethinkdb_gen").mkdirs()
            delete(checkout_dir, proto_folder, tests_folder)
            exec {
                commandLine("git", "clone", "--progress", "-b", rethinkdb_branch, "--single-branch", rethinkdb_repo, checkout_dir)
            }
            exec {
                commandLine("cp", "-a", "-R", "$checkout_dir/$proto_location/.", proto_folder.absolutePath)
            }
            exec {
                commandLine("cp", "-a", "-R", "$checkout_dir/$tests_location/.", tests_folder.absolutePath)
            }
        }
    }

    val genMainJava by creating {
        group = "code generation"
        description = "Generates java files for the driver."
    }

    val genTestJava by creating {
        group = "code generation"
        description = "Generates test files for the driver."

        //properties
        val tests_target = findProperty("build.tests.target_folder")
        val src_test = findProperty("build.gen.src.test")
        val templates = findProperty("build.gen.src.templates")

        val tests_folder = File(buildDir, "rethinkdb_gen/$tests_target")
        val src_test_gen = File("$src_test/gen")

        doLast {
            delete(src_test_gen)
            exec {
                setStandardOutput(System.err)
                commandLine("python3",
                    "scripts/convert_tests.py",
                    "--debug",
                    "--test-dir=${tests_folder.absolutePath}",
                    "--test-output-dir=${src_test_gen.absolutePath}",
                    "--template-dir=$templates"
                )
            }
        }
    }
}