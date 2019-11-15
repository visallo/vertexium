package org.vertexium.elasticsearch7.lucene;

import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;

public class ListQueryStringNode implements QueryStringNode {
    private List<QueryStringNode> children = new ArrayList<>();

    public void add(QueryStringNode queryStringNode) {
        children.add(queryStringNode);
    }

    public List<QueryStringNode> getChildren() {
        return children;
    }

    @Override
    public String toString() {
        return "ListQueryStringNode{" +
            "children=" + Joiner.on(", ").join(children) +
            '}';
    }
}
