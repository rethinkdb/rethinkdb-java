@file:Repository(url = "https://jcenter.bintray.com")
@file:Repository(url = "https://dl.bintray.com/adriantodt/maven")
@file:DependsOn("pw.aru.libs:properties:1.2")
@file:DependsOn("com.github.adriantodt:tartar:1.1.1")
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
    ASSIGN,
    SEMICOLON,
    LBRACKET,
    RBRACKET,
    LBRACE,
    RBRACE,

    STRING,
    INTEGER,
    DECIMAL,
    BOOLEAN,

    IDENTIFIER,
    MESSAGE,
    ENUM,
    SYNTAX,
    OPTIONAL,
    REPEATED,

    INVALID
}

sealed class ProtoExpr {
    object Ignore : ProtoExpr()
    data class ModuleNode(val moduleName: String, val child: List<ProtoExpr>) : ProtoExpr()
    data class EnumNode(val enumName: String, val child: List<ProtoExpr>) : ProtoExpr()
    data class EnumValueExpr(val name: String, val value: Int) : ProtoExpr()
    data class ModuleValueExpr(
        val type: String, val name: String, val value: Int,
        val modifier: Modifier? = null, val options: Map<String, Any>? = null
    ) : ProtoExpr() {
        enum class Modifier {
            OPTIONAL, REPEATED
        }
    }
}

val properties = Properties.fromFile("scripts.properties")
val source = Source(File(properties["sources.protofile.targetFile"]!!))

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
            is LexicalNumber.Invalid -> makeToken(TokenType.INVALID, n.string)
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
        eat(TokenType.ASSIGN)
        eat(TokenType.STRING)
        eat(TokenType.SEMICOLON)
        ProtoExpr.Ignore
    }
    prefix(TokenType.MESSAGE) {
        val name = eat(TokenType.IDENTIFIER).value
        eat(TokenType.LBRACKET)
        val stmt = ArrayList<ProtoExpr>()
        while (!match(TokenType.RBRACKET)) stmt += parseExpression()
        stmt.removeIf(ProtoExpr.Ignore::equals)
        ProtoExpr.ModuleNode(name, stmt)
    }
    prefix(TokenType.ENUM) {
        val name = eat(TokenType.IDENTIFIER).value
        eat(TokenType.LBRACKET)
        val stmt = ArrayList<ProtoExpr>()
        while (!match(TokenType.RBRACKET)) stmt += parseExpression()
        stmt.removeIf(ProtoExpr.Ignore::equals)
        ProtoExpr.ModuleNode(name, stmt)
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
            val type = it.value
            val name = eat().value
            eat(TokenType.ASSIGN)
            val value = eat(TokenType.INTEGER).value.toInt()
            val options = if (match(TokenType.LBRACE)) {
                val optionName = eat().value
                eat(TokenType.ASSIGN)
                val optionValue: Any = eat().let { optVal ->
                    when (optVal.type) {
                        TokenType.STRING -> optVal.value.removeSurrounding("\"")
                        TokenType.INTEGER -> optVal.value.toInt()
                        TokenType.BOOLEAN -> optVal.value.toBoolean()
                        else -> throw SyntaxException("Unexpected $optVal", optVal.section)
                    }
                }
                eat(TokenType.RBRACE)
                mapOf(optionName to optionValue)
            } else null
            eat(TokenType.SEMICOLON)
            ProtoExpr.ModuleValueExpr(type, name, value, options = options)
        } else {
            val name = it.value
            eat(TokenType.ASSIGN)
            val value = eat(TokenType.INTEGER).value.toInt()
            eat(TokenType.SEMICOLON)
            ProtoExpr.EnumValueExpr(name, value)
        }
    }
}

val parser = createParser(grammar) {
    val stmt = ArrayList<ProtoExpr>()
    while (!eof) stmt += parseExpression()
    stmt.removeIf(ProtoExpr.Ignore::equals)
    stmt
}

ObjectMapper().writeValueAsString(parser.parse(source, lexer))