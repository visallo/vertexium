package org.vertexium.elasticsearch7.lucene;

public class ClauseQueryStringNode implements QueryStringNode {
    private final Token field;
    private final QueryStringNode child;
    private final Token boost;
    private final boolean includeParenthesis;

    public ClauseQueryStringNode(Token field, QueryStringNode child) {
        this(field, child, null, false);
    }

    public ClauseQueryStringNode(Token field, QueryStringNode child, Token boost, boolean includeParenthesis) {
        this.field = field;
        this.child = child;
        this.boost = boost;
        this.includeParenthesis = includeParenthesis;
    }

    public Token getField() {
        return field;
    }

    public QueryStringNode getChild() {
        return child;
    }

    public Token getBoost() {
        return boost;
    }

    public boolean isIncludeParenthesis() {
        return includeParenthesis;
    }

    @Override
    public String toString() {
        return "ClauseQueryStringNode{" +
            "field=" + field +
            ", child=" + child +
            ", boost=" + boost +
            '}';
    }
}
