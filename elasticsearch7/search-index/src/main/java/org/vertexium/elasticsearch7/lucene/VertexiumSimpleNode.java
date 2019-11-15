package org.vertexium.elasticsearch7.lucene;

public class VertexiumSimpleNode extends SimpleNode implements QueryStringNode {
    public VertexiumSimpleNode(int i) {
        super(i);
    }

    public VertexiumSimpleNode(QueryParser p, int i) {
        super(p, i);
    }

    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder();
        ret.append("VertexiumSimpleNode{");
        for (Token t = jjtGetFirstToken(); ; t = t.next) {
            if (t == null) {
                break;
            }
            ret.append(t.image);
            if (t == jjtGetLastToken()) {
                break;
            }
        }
        ret.append("}");
        return ret.toString();
    }
}
