package org.vertexium.elasticsearch7.lucene;

public class BooleanQueryStringNode implements QueryStringNode {
    private final String conjunction;
    private final String modifiers;
    private final ClauseQueryStringNode clause;

    public BooleanQueryStringNode(String conjunction, String modifiers, ClauseQueryStringNode clause) {
        this.conjunction = conjunction;
        this.modifiers = modifiers;
        this.clause = clause;
    }

    public String getConjunction() {
        return conjunction;
    }

    public String getModifiers() {
        return modifiers;
    }

    public ClauseQueryStringNode getClause() {
        return clause;
    }

    @Override
    public String toString() {
        return "BooleanQueryStringNode{" +
            "conjunction='" + conjunction + '\'' +
            ", modifiers='" + modifiers + '\'' +
            ", clause=" + clause +
            '}';
    }
}
