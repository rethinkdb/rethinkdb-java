@file:Repository(url = "https://jcenter.bintray.com")
@file:Repository(url = "https://dl.bintray.com/adriantodt/maven")
@file:DependsOn("pw.aru.libs:properties:1.2")
@file:DependsOn("com.github.adriantodt:tartar:1.1.2")
@file:DependsOn("com.fasterxml.jackson.core:jackson-databind:2.10.2")

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.adriantodt.tartar.api.lexer.Source
import com.github.adriantodt.tartar.api.parser.SyntaxException
import com.github.adriantodt.tartar.api.parser.Token
import com.github.adriantodt.tartar.createGrammar
import com.github.adriantodt.tartar.createLexer
import com.github.adriantodt.tartar.createParser
import com.github.adriantodt.tartar.extensions.*
import pw.aru.libs.properties.Properties
import java.io.File

enum class TokenType {
    LBRACKET, RBRACKET, LBRACE, RBRACE, ASSIGN, SEMICOLON,
    STRING, INTEGER, DECIMAL, BOOLEAN,
    IDENTIFIER, MESSAGE, ENUM, SYNTAX, OPTIONAL, REPEATED
}

sealed class ProtoExpr {
    object Ignore : ProtoExpr()
    data class ModuleNode(val moduleName: String, val child: List<ProtoExpr>) : ProtoExpr()
    data class EnumNode(val enumName: String, val child: List<ProtoExpr>) : ProtoExpr()
    data class EnumValueExpr(val name: String, val value: Int) : ProtoExpr()
    data class ModuleValueExpr(
        val type: String, val name: String, val value: Int, val options: Map<String, Any>?, val modifier: Modifier?
    ) : ProtoExpr() {
        enum class Modifier { OPTIONAL, REPEATED }
    }
}

val lexer = createLexer<Token<TokenType>> {
    ' '()
    '\n'()
    '=' { process(makeToken(TokenType.ASSIGN)) }
    ';' { process(makeToken(TokenType.SEMICOLON)) }
    '{' { process(makeToken(TokenType.LBRACKET)) }
    '}' { process(makeToken(TokenType.RBRACKET)) }
    '[' { process(makeToken(TokenType.LBRACE)) }
    ']' { process(makeToken(TokenType.RBRACE)) }
    "//" { while (hasNext()) if (next() == '\n') break }
    "/*" { while (hasNext()) if (next() == '*' && match('/')) break }
    '"' { process(makeToken(TokenType.STRING, readString(it))) }
    matching { it.isDigit() }.configure {
        process(when (val n = readNumber(it)) {
            is LexicalNumber.Invalid -> throw SyntaxException("Invalid number '${n.string}'", section(n.string.length))
            is LexicalNumber.Decimal -> makeToken(TokenType.DECIMAL, n.value.toString())
            is LexicalNumber.Integer -> makeToken(TokenType.INTEGER, n.value.toString())
        })
    }
    matching { it.isLetter() || it == '_' }.configure {
        process(when (val s = readIdentifier(it)) {
            "syntax" -> makeToken(TokenType.SYNTAX, s)
            "message" -> makeToken(TokenType.MESSAGE, s)
            "enum" -> makeToken(TokenType.ENUM, s)
            "repeated" -> makeToken(TokenType.REPEATED, s)
            "optional" -> makeToken(TokenType.OPTIONAL, s)
            "true", "false" -> makeToken(TokenType.BOOLEAN, s)
            else -> makeToken(TokenType.IDENTIFIER, s)
        })
    }
}

val grammar = createGrammar<TokenType, ProtoExpr> {
    prefix(TokenType.SYNTAX) {
        eatMulti(TokenType.ASSIGN, TokenType.STRING, TokenType.SEMICOLON)
        ProtoExpr.Ignore
    }
    prefix(TokenType.MESSAGE) {
        val (name) = eatMulti(TokenType.IDENTIFIER, TokenType.LBRACKET)
        val stmt = ArrayList<ProtoExpr>()
        while (!match(TokenType.RBRACKET)) stmt += parseExpression()
        stmt.removeIf(ProtoExpr.Ignore::equals)
        ProtoExpr.ModuleNode(name.value, stmt)
    }
    prefix(TokenType.ENUM) {
        val (name) = eatMulti(TokenType.IDENTIFIER, TokenType.LBRACKET)
        val stmt = ArrayList<ProtoExpr>()
        while (!match(TokenType.RBRACKET)) stmt += parseExpression()
        stmt.removeIf(ProtoExpr.Ignore::equals)
        ProtoExpr.EnumNode(name.value, stmt)
    }
    prefix(TokenType.OPTIONAL) {
        val m = parseExpression() as? ProtoExpr.ModuleValueExpr
            ?: throw SyntaxException("Expected a module value", it.section)
        m.copy(modifier = ProtoExpr.ModuleValueExpr.Modifier.OPTIONAL)
    }
    prefix(TokenType.REPEATED) {
        val m = parseExpression() as? ProtoExpr.ModuleValueExpr
            ?: throw SyntaxException("Expected a module value", it.section)
        m.copy(modifier = ProtoExpr.ModuleValueExpr.Modifier.REPEATED)
    }
    prefix(TokenType.IDENTIFIER) {
        if (nextIs(TokenType.IDENTIFIER)) {
            val (name, _, value) = eatMulti(TokenType.IDENTIFIER, TokenType.ASSIGN, TokenType.INTEGER)
            val options = if (match(TokenType.LBRACE)) {
                val (optionKey) = eatMulti(TokenType.IDENTIFIER, TokenType.ASSIGN)
                val optionValue: Any = eat().let { optVal ->
                    when (optVal.type) {
                        TokenType.STRING -> optVal.value
                        TokenType.INTEGER -> optVal.value.toInt()
                        TokenType.BOOLEAN -> optVal.value.toBoolean()
                        else -> throw SyntaxException("Unexpected $optVal", optVal.section)
                    }
                }
                eat(TokenType.RBRACE)
                mapOf(optionKey.value to optionValue)
            } else null
            eat(TokenType.SEMICOLON)
            ProtoExpr.ModuleValueExpr(it.value, name.value, value.value.toInt(), options, null)
        } else {
            val (_, value) = eatMulti(TokenType.ASSIGN, TokenType.INTEGER, TokenType.SEMICOLON)
            ProtoExpr.EnumValueExpr(it.value, value.value.toInt())
        }
    }
}

val parser = createParser(grammar) {
    val stmt = ArrayList<ProtoExpr>()
    while (!eof) stmt += parseExpression()
    stmt.removeIf(ProtoExpr.Ignore::equals)
    stmt
}

val properties = Properties.fromFile("scripts.properties")
val source = Source(File(properties["sources.protofile.targetFile"]!!))
val target = File(properties["convert.protofile.targetFile"]!!)
target.parentFile.mkdirs()
target.delete()

ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(target, parser.parse(source, lexer))