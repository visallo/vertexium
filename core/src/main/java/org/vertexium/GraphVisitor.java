package org.vertexium;

public interface GraphVisitor {
    void visitElement(Element element);

    void visitVertex(Vertex vertex);

    void visitEdge(Edge edge);

    void visitProperty(Element element, Property property);

    void visitExtendedDataRow(Element element, String tableName, ExtendedDataRow row);

    void visitProperty(Element element, String tableName, ExtendedDataRow row, Property property);
}
