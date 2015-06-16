package org.vertexium.cli.model;

import org.vertexium.cli.VertexiumScript;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

public class LazyVertexList extends ModelBase {
    private final List<String> vertexIds;

    public LazyVertexList(List<String> vertexIds) {
        this.vertexIds = vertexIds;
    }

    @Override
    public String toString() {
        StringWriter out = new StringWriter();
        PrintWriter writer = new PrintWriter(out);

        VertexiumScript.getContextVertices().clear();
        int vertexIndex = 0;

        writer.println("");
        for (String vertexId : vertexIds) {
            String vertexIndexString = "v" + vertexIndex;
            writer.println("@|bold " + vertexIndexString + ":|@ " + vertexId);
            LazyVertex lazyVertex = new LazyVertex(vertexId);
            VertexiumScript.getContextVertices().put(vertexIndexString, lazyVertex);
            vertexIndex++;
        }

        return out.toString();
    }
}
