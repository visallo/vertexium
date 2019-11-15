package org.vertexium.elasticsearch7.lucene;

public class StringQueryStringNode implements QueryStringNode {
    private final String value;

    public StringQueryStringNode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "StringQueryStringNode{" +
            "value='" + value + '\'' +
            '}';
    }
}
