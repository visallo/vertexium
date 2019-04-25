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
        CypherParser.OC_CypherContext tree = parser.oC_Cypher();
        String treeText = tree.getText();
        if (treeText.endsWith("<EOF>")) {
            treeText = treeText.substring(0, treeText.length() - "<EOF>".length());
        }
        if (!treeText.equals(code)) {
            throw new VertexiumCypherSyntaxErrorException("Parsing error, \"" + code.substring(treeText.length()) + "\"");
        }
        return new CypherCstToAstVisitor(ctx).visitOC_Cypher(tree);
    }

    public CypherAstBase parseExpression(String expressionString) {
        CypherLexer lexer = new CypherLexer(CharStreams.fromString(expressionString));
        CypherParser parser = new CypherParser(new CommonTokenStream(lexer));
        parser.setErrorHandler(new ParserErrorHandler(expressionString));
        CypherParser.OC_ExpressionContext expressionContext = parser.oC_Expression();
        return new CypherCstToAstVisitor().visitOC_Expression(expressionContext);
    }

    private static class ParserErrorHandler extends BailErrorStrategy {
        private final String code;

        public ParserErrorHandler(String code) {
            this.code = code;
        }

        @Override
        public void reportError(Parser recognizer, RecognitionException e) {
            String messagePrefix = "";
            if (e.getCtx() instanceof CypherParser.OC_RelationshipPatternContext) {
                messagePrefix = "InvalidRelationshipPattern: ";
            }
            throw new VertexiumCypherSyntaxErrorException(
                String.format(
                    "%sCould not parse (%d:%d): %s",
                    messagePrefix,
                    e.getOffendingToken().getLine(),
                    e.getOffendingToken().getCharPositionInLine(),
                    code
                ),
                e);
        }
    }
}
