package org.vertexium.cypher;

import org.vertexium.Edge;
import org.vertexium.Element;
import org.vertexium.Vertex;

import java.util.List;

public class RelationshipRangePathResult extends PathResultBase {
    public RelationshipRangePathResult(Vertex source, Edge edge) {
        super(source, edge);
    }

    public RelationshipRangePathResult(List<Element> elements) {
        super(elements);
    }

    public RelationshipRangePathResult(PathResultBase path, Element element) {
        super(path, element);
    }

    public RelationshipRangePathResult() {
    }
}
