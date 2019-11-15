package org.vertexium.elasticsearch7.lucene;

import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;

public class MultiTermQueryStringNode implements QueryStringNode {
    private final List<Token> tokens = new ArrayList<>();

    public void add(Token token) {
        tokens.add(token);
    }

    public List<Token> getTokens() {
        return tokens;
    }

    @Override
    public String toString() {
        return "MultiTermQueryStringNode{" +
            "tokens=" + Joiner.on(", ").join(tokens) +
            '}';
    }
}
