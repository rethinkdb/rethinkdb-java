import java.util.Properties
import java.io.File
import com.jfrog.bintray.gradle.BintrayExtension
import com.jfrog.bintray.gradle.tasks.BintrayUploadTask
import com.jfrog.bintray.gradle.tasks.RecordingCopyTask


plugins {
    java
    maven
    `maven-publish`
    signing
    id("com.jfrog.bintray") version "1.8.4"
}

version = "2.4.3"
group = "com.rethinkdb"

java.sourceCompatibility = JavaVersion.VERSION_1_8
java.targetCompatibility = JavaVersion.VERSION_1_8

repositories {
    mavenCentral()
}

dependencies {
    testCompile("junit:junit:4.12")
    testCompile("net.jodah:concurrentunit:0.4.6")
    testRuntime("ch.qos.logback:logback-classic:1.2.3")
    compile("org.slf4j:slf4j-api:1.7.30")
    compile("org.jetbrains:annotations:19.0.0")
    compile("com.fasterxml.jackson.core:jackson-databind:2.10.2")
}

file("confidential.properties").takeIf(File::exists)?.let {
    val properties = Properties()
    it.inputStream().use(properties::load)
    allprojects { properties.forEach { name, value -> extra.set(name.toString(), value) } }
}

gradle.taskGraph.whenReady {
    val hasUploadArchives = hasTask(":uploadArchives")
    val hasBintrayUpload = hasTask(":bintrayUpload")
    val hasDoSigning = hasTask(":doSigning")
    signing.isRequired = hasBintrayUpload || hasUploadArchives || hasDoSigning
}

fun findProperty(s: String) = project.findProperty(s) as String?
tasks {
    val doSigning by creating {
        dependsOn("signArchives")
    }
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

    val downloadProtoAndTests by creating {
        group = "build setup"
        description = "Downloads contents from rethinkdb main repository."

        //properties
        val rethinkdb_repo = findProperty("build.rethinkdb_repo")!!
        val rethinkdb_branch = findProperty("build.rethinkdb_branch")!!
        val checkout_dir = findProperty("build.rethinkdb_checkout_dir")!!
        val proto_location = findProperty("build.proto.src_location")!!
        val proto_target = findProperty("build.proto.target_folder")!!
        val tests_location = findProperty("build.tests.src_location")!!
        val tests_target = findProperty("build.tests.target_folder")!!

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

    val generateJsonFiles by creating {
        group = "code generation"
        description = "Generates json files for the java file generation."

        val convert_proto = findProperty("build.gen.py.convert_proto")!!
        val metajava = findProperty("build.gen.py.metajava")!!
        val json_target = findProperty("build.gen.json.target_folder")!!
        val proto_folder = findProperty("build.proto.target_folder")!!
        val proto_name = findProperty("build.proto.file_name")!!
        val proto_basic_name = findProperty("build.gen.json.proto_basic")!!
        val term_info_name = findProperty("build.gen.json.term_info")!!
        val java_term_info_name = findProperty("build.gen.json.java_term_info")!!

        val json_folder = File(buildDir, "rethinkdb_gen/$json_target")
        val proto_file = File(buildDir, "rethinkdb_gen/$proto_folder/$proto_name")
        val proto_basic = File(buildDir, "rethinkdb_gen/$json_target/$proto_basic_name")
        val term_info = File(buildDir, "rethinkdb_gen/$json_target/$term_info_name")
        val java_term_info = File(buildDir, "rethinkdb_gen/$json_target/$java_term_info_name")

        doLast {
            File(buildDir, "rethinkdb_gen").mkdirs()
            delete(json_folder)
            json_folder.mkdirs()
            exec {
                standardOutput = System.err
                commandLine("python3", convert_proto,
                    proto_file,
                    proto_basic
                )
            }
            exec {
                standardOutput = System.err
                commandLine("python3", metajava, "update-terminfo",
                    "--proto-json=$proto_basic",
                    "--term-info=$term_info"
                )
            }
            exec {
                standardOutput = System.err
                commandLine("python3", metajava, "generate-java-terminfo",
                    "--term-info=$term_info",
                    "--output-file=$java_term_info"
                )
            }
        }
    }

    val genMainJava by creating {
        group = "code generation"
        description = "Generates java files for the driver."

        val localFiles = findProperty("build.gen.use_local_files")!!.toBoolean()

        enabled = localFiles // TODO enable this once we fix update-terminfo

        val metajava = findProperty("build.gen.py.metajava")!!
        val json_target = if (localFiles) "../../scripts" else findProperty("build.gen.json.target_folder")!!
        val proto_basic_name = findProperty("build.gen.json.proto_basic")!!
        val global_info = findProperty("build.json.global_info")!!
        val java_term_info_name = findProperty("build.gen.json.java_term_info")!!
        val src_main = findProperty("build.gen.src.main")!!
        val templates = findProperty("build.gen.src.templates")!!
        val folders = findProperty("build.gen.src.main.packages")!!.split(',')

        val proto_basic = File(buildDir, "rethinkdb_gen/$json_target/$proto_basic_name")
        val java_term_info = File(buildDir, "rethinkdb_gen/$json_target/$java_term_info_name")
        val src_main_gen = File("$src_main/gen")


        doLast {
            delete(src_main_gen)
            folders.forEach { File(src_main_gen, it).mkdirs() }
            exec {
                standardOutput = System.err
                commandLine("python3", metajava, "generate-java-classes",
                    "--global-info=$global_info",
                    "--proto-json=$proto_basic",
                    "--java-term-info=$java_term_info",
                    "--template-dir=$templates",
                    "--package-dir=$src_main"
                )
            }
        }
    }

    val genTestJava by creating {
        group = "code generation"
        description = "Generates test files for the driver."

        //properties
        val convert_tests = findProperty("build.gen.py.convert_tests")!!
        val tests_target = findProperty("build.tests.target_folder")!!
        val src_test = findProperty("build.gen.src.test")!!
        val templates = findProperty("build.gen.src.templates")!!

        val tests_folder = File(buildDir, "rethinkdb_gen/$tests_target")
        val src_test_gen = File("$src_test/gen")

        doLast {
            delete(src_test_gen)
            src_test_gen.mkdirs()
            exec {
                standardOutput = System.err
                commandLine("python3", convert_tests, "--debug",
                    "--test-dir=$tests_folder",
                    "--test-output-dir=$src_test_gen",
                    "--template-dir=$templates"
                )
            }
        }
    }


    getByName<Upload>("uploadArchives") {
        repositories {
            withConvention(MavenRepositoryHandlerConvention::class) {
                mavenDeployer {
                    beforeDeployment { signing.signPom(this) }

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
                                    "id"("adriantodt")
                                    "name"("Adrian Todt")
                                    "email"("adriantodt.ms@gmail.com")
                                }
                                "developer" {
                                    "id"("gabor-boros")
                                    "name"("Gábor Boros")
                                    "email"("gabor@rethinkdb.com")
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    publishing {
        publications.create("mavenJava", MavenPublication::class.java) {
            groupId = project.group.toString()
            artifactId = project.name
            version = project.version.toString()

            from(project.components["java"])
            artifact(sourcesJar)
            artifact(javadocJar)

            pom.withXml {
                val root = asNode()
                root.appendNode("name", "RethinkDB Java Driver")
                root.appendNode("packaging", "jar")
                root.appendNode("description", "Official Java driver for RethinkDB")
                root.appendNode("url", "http://rethinkdb.com")

                val scm = root.appendNode("scm")
                scm.appendNode("connection","scm:git:https://github.com/rethinkdb/rethinkdb-java")
                scm.appendNode("developerConnection","scm:git:https://github.com/rethinkdb/rethinkdb-java")
                scm.appendNode("url", "https://github.com/rethinkdb/rethinkdb-java")

                val license = root.appendNode("licenses").appendNode("license")
                license.appendNode("name","The Apache License, Version 2.0")
                license.appendNode("url","http://www.apache.org/licenses/LICENSE-2.0.txt")

                val developers = root.appendNode("developers")

                val dev1 = developers.appendNode("developer")
                dev1.appendNode("id","adriantodt")
                dev1.appendNode("name","Adrian Todt")
                dev1.appendNode("email","adriantodt.ms@gmail.com")

                val dev2 = developers.appendNode("developer")
                dev2.appendNode("id","gabor-boros")
                dev2.appendNode("name","Gábor Boros")
                dev2.appendNode("email","gabor@rethinkdb.com")
            }
        }
    }

    withType<BintrayUploadTask> {
        dependsOn("assemble", "publishToMavenLocal")
        mustRunAfter("doSigning", "signArchives")
    }
}

signing {
    // Don't sign unless this is a release version
    sign(configurations.archives.get())
    sign(publishing.publications.get("mavenJava"))
}

bintray {
    user = findProperty("bintray.user")
    key = findProperty("bintray.key")
    publish = true
    setPublications("mavenJava")

    filesSpec(delegateClosureOf<RecordingCopyTask> {
        into("com/rethinkdb/${project.name}/${project.version}/")

        from("${buildDir}/libs/") {
            include("*.jar.asc")
        }

        from("${buildDir}/publications/mavenJava/") {
            include("pom-default.xml.asc")
            rename("pom-default.xml.asc", "${project.name}-${project.version}.pom.asc")
        }
    })

    pkg(delegateClosureOf<BintrayExtension.PackageConfig> {
        repo = "maven"
        name = project.name
        userOrg = "rethinkdb"
        setLicenses("Apache-2.0")
        vcsUrl = "https://github.com/rethinkdb/rethinkdb-java.git"
        version(delegateClosureOf<BintrayExtension.VersionConfig> {
            mavenCentralSync(delegateClosureOf<BintrayExtension.MavenCentralSyncConfig> {
                user = findProperty("ossrhUsername")
                password = findProperty("ossrhPassword")
                sync = !user.isNullOrBlank() && !password.isNullOrBlank()
            })
        })
    })
}
