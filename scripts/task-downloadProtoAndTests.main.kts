@file:Repository(url = "https://jcenter.bintray.com")
@file:DependsOn("pw.aru.libs:properties:1.2")

import pw.aru.libs.properties.Properties
import java.io.File
import kotlin.system.exitProcess

fun system(vararg command: String) = ProcessBuilder().inheritIO().command(*command).start().waitFor()

val properties = Properties.fromFile("scripts.properties")

// Delete all target folders
listOfNotNull(
    properties["sources.checkoutDir"],
    properties["sources.protofile.targetFolder"],
    properties["sources.tests.targetFolder"]
).forEach { File(it).deleteRecursively() }

// Git clone
val checkoutDir = properties["sources.checkoutDir"] ?: "/tmp/rethinkdb"
system(
    "git", "clone", "--progress", "-b", properties["sources.rethinkdbBranch"] ?: "next", "--single-branch",
    properties["sources.rethinkdbRepo"]!!, checkoutDir
)

// Move protofile
File(checkoutDir, properties["sources.protofile.sourceFile"]!!).copyTo(
    File(properties["sources.protofile.targetFile"]!!).apply { parentFile.mkdirs() }
)

// Move test source folder
File(checkoutDir, properties["sources.tests.sourceFolder"]!!).copyRecursively(
    File(properties["sources.tests.targetFolder"]!!).apply { parentFile.mkdirs() }
)

exitProcess(0)