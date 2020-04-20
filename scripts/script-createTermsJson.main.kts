@file:Repository(url = "https://jcenter.bintray.com")
@file:Repository(url = "https://dl.bintray.com/adriantodt/maven")
@file:DependsOn("pw.aru.libs:properties:1.2")
@file:DependsOn("com.fasterxml.jackson.core:jackson-databind:2.10.2")
@file:Suppress("UNCHECKED_CAST")

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import pw.aru.libs.properties.Properties
import java.io.File

val properties = Properties.fromFile("scripts.properties")
val target = File(properties["convert.protofile.targetFile"]!!)

val mapper = ObjectMapper()

val ql2 = mapper.readValue(target, object : TypeReference<List<Map<String, Any>>>() {})
val term = ql2.first { it["moduleName"] == "Term" }["child"] as List<Map<String, Any>>
val termType = term.first { it["enumName"] == "TermType" }["child"] as List<Map<String, Any>>

val folder = File("scripts/terms")
folder.mkdirs()

termType.forEach {
    val name = it["name"].toString().toLowerCase() + ".json"
    val map = mapOf("name" to it["name"], "id" to it["value"])
    mapper.writerWithDefaultPrettyPrinter().writeValue(File(folder, name), map)
}