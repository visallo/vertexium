package org.vertexium.cypher.ast;

import org.antlr.v4.runtime.*;
import org.vertexium.cypher.CypherLexer;
import org.vertexium.cypher.CypherParser;
import org.vertexium.cypher.ast.model.CypherAstBase;
import org.vertexium.cypher.ast.model.CypherStatement;
import org.vertexium.cypher.exceptions.VertexiumCypherSyntaxErrorException;

public class CypherAstParser {
    private static final CypherAstParser instance = new CypherAstParser();

    public static CypherAstParser getInstance() {
        return instance;
    }

    public CypherStatement parse(CypherCompilerContext ctx, String code) {
        CodePointCharStream input = CharStreams.fromString(code);
        CypherLexer lexer = new CypherLexer(input);
        CypherParser parser = new CypherParser(new CommonTokenStream(lexer));
        parser.setErrorHandler(new ParserErrorHandler(code));
        CypherParser.CypherContext tree = parser.cypher();
        if (!tree.getText().equals(code)) {
            throw new VertexiumCypherSyntaxErrorException("Parsing error, \"" + code.substring(tree.getText().length()) + "\"");
        }
        return new CypherCstToAstVisitor(ctx).visitCypher(tree);
    }

    public CypherAstBase parseExpression(String expressionString) {
        CypherLexer lexer = new CypherLexer(CharStreams.fromString(expressionString));
        CypherParser parser = new CypherParser(new CommonTokenStream(lexer));
        parser.setErrorHandler(new ParserErrorHandler(expressionString));
        CypherParser.ExpressionContext expressionContext = parser.expression();
        return new CypherCstToAstVisitor().visitExpression(expressionContext);
    }

    private static class ParserErrorHandler extends BailErrorStrategy {
        private final String code;

        public ParserErrorHandler(String code) {
            this.code = code;
        }

        @Override
        public void reportError(Parser recognizer, RecognitionException e) {
            String messagePrefix = "";
            if (e.getCtx() instanceof CypherParser.RelationshipPatternContext) {
                messagePrefix = "InvalidRelationshipPattern: ";
            }
            throw new VertexiumCypherSyntaxErrorException(messagePrefix + "Could not parse: " + code, e);
        }
    }
}
