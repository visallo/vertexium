package org.vertexium.elasticsearch7.lucene;

import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.GraphWithSearchIndex;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch7.Elasticsearch7SearchIndex;

import java.util.Locale;

public class DefaultQueryStringTransformer implements QueryStringTransformer {
    private final Graph graph;

    public DefaultQueryStringTransformer(Graph graph) {
        this.graph = graph;
    }

    public String transform(String queryString, Authorizations authorizations) {
        if (queryString == null) {
            return queryString;
        }
        queryString = queryString.trim();
        if (queryString.length() == 0 || "*".equals(queryString)) {
            return queryString;
        }

        LuceneQueryParser parser = new LuceneQueryParser(queryString);
        QueryStringNode node = parser.parse();
        return visit(node, authorizations);
    }

    protected String visit(QueryStringNode node, Authorizations authorizations) {
        if (node instanceof StringQueryStringNode) {
            return visitStringQueryStringNode((StringQueryStringNode) node);
        } else if (node instanceof ListQueryStringNode) {
            return visitListQueryStringNode((ListQueryStringNode) node, authorizations);
        } else if (node instanceof BooleanQueryStringNode) {
            return visitBooleanQueryStringNode((BooleanQueryStringNode) node, authorizations);
        } else if (node instanceof ClauseQueryStringNode) {
            return visitClauseQueryStringNode((ClauseQueryStringNode) node, authorizations);
        } else if (node instanceof VertexiumSimpleNode) {
            return visitVertexiumSimpleNode((VertexiumSimpleNode) node);
        } else if (node instanceof MultiTermQueryStringNode) {
            return visitMultiTermQueryStringNode((MultiTermQueryStringNode) node);
        } else {
            throw new VertexiumException("Unsupported query string node type: " + node.getClass().getName());
        }
    }

    protected String visitMultiTermQueryStringNode(MultiTermQueryStringNode node) {
        StringBuilder ret = new StringBuilder();
        boolean first = true;
        for (Token token : node.getTokens()) {
            if (!first) {
                ret.append(" ");
            }
            ret.append(token.image);
            first = false;
        }
        return ret.toString();
    }

    protected String visitVertexiumSimpleNode(VertexiumSimpleNode vertexiumSimpleNode) {
        StringBuilder ret = new StringBuilder();
        for (Token t = vertexiumSimpleNode.jjtGetFirstToken(); ; t = t.next) {
            if (t == null) {
                break;
            }
            if (t.kind == QueryParserConstants.RANGE_TO) {
                ret.append(" TO ");
            } else {
                ret.append(t.image);
            }
            if (t == vertexiumSimpleNode.jjtGetLastToken()) {
                break;
            }
        }
        return ret.toString();
    }

    protected String visitClauseQueryStringNode(ClauseQueryStringNode clauseQueryStringNode, Authorizations authorizations) {
        StringBuilder ret = new StringBuilder();
        if (clauseQueryStringNode.getField() != null) {
            String fieldName = EscapeQuerySyntax.discardEscapeChar(clauseQueryStringNode.getField().image);
            fieldName = cleanupFieldName(fieldName);

            String[] fieldNames = expandFieldName(fieldName, authorizations);
            if (fieldNames == null || fieldNames.length == 0) {
                ret.append(clauseQueryStringNode.getField().image).append(":");
            } else if (fieldNames.length == 1) {
                ret.append(EscapeQuerySyntax.escapeTerm(fieldNames[0], Locale.getDefault())).append(":");
            } else {
                boolean first = true;
                ret.append("(");
                for (String propertyName : fieldNames) {
                    if (!first) {
                        ret.append(" OR ");
                    }
                    ret.append(EscapeQuerySyntax.escapeTerm(propertyName, Locale.getDefault())).append(":");
                    visitClauseQueryStringNodeValue(clauseQueryStringNode, ret, authorizations);
                    first = false;
                }
                ret.append(")");
                return ret.toString();
            }
        }
        visitClauseQueryStringNodeValue(clauseQueryStringNode, ret, authorizations);
        return ret.toString();
    }

    protected void visitClauseQueryStringNodeValue(ClauseQueryStringNode clauseQueryStringNode, StringBuilder ret, Authorizations authorizations) {
        if (clauseQueryStringNode.isIncludeParenthesis() || clauseQueryStringNode.getBoost() != null) {
            ret.append("(")
                .append(visit(clauseQueryStringNode.getChild(), authorizations))
                .append(")");
        } else {
            ret.append(visit(clauseQueryStringNode.getChild(), authorizations));
        }

        if (clauseQueryStringNode.getBoost() != null) {
            ret.append("^").append(clauseQueryStringNode.getBoost().toString());
        }
    }

    protected String visitBooleanQueryStringNode(BooleanQueryStringNode booleanQueryStringNode, Authorizations authorizations) {
        StringBuilder ret = new StringBuilder();
        if (booleanQueryStringNode.getConjunction() != null) {
            ret.append(booleanQueryStringNode.getConjunction()).append(" ");
        }
        if (booleanQueryStringNode.getModifiers() != null) {
            ret.append(booleanQueryStringNode.getModifiers());
        }
        ret.append(visit(booleanQueryStringNode.getClause(), authorizations));
        return ret.toString();
    }

    protected String visitListQueryStringNode(ListQueryStringNode listQueryStringNode, Authorizations authorizations) {
        StringBuilder ret = new StringBuilder();
        boolean first = true;
        for (QueryStringNode child : listQueryStringNode.getChildren()) {
            if (!first) {
                ret.append(" ");
            }
            ret.append(visit(child, authorizations));
            first = false;
        }
        return ret.toString();
    }

    protected String visitStringQueryStringNode(StringQueryStringNode stringQueryStringNode) {
        return stringQueryStringNode.getValue();
    }

    protected String[] expandFieldName(String fieldName, Authorizations authorizations) {
        return getSearchIndex().getPropertyNames(graph, fieldName, authorizations);
    }

    protected String cleanupFieldName(String fieldName) {
        fieldName = fieldName.trim();
        if (fieldName.startsWith("\"") && fieldName.endsWith("\"")) {
            fieldName = fieldName.substring(1, fieldName.length() - 1);
        }
        return fieldName;
    }

    public Elasticsearch7SearchIndex getSearchIndex() {
        return (Elasticsearch7SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
    }

    public Graph getGraph() {
        return graph;
    }
}
