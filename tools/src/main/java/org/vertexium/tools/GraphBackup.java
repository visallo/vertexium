package org.vertexium.tools;

import com.beust.jcommander.Parameter;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;
import org.vertexium.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.util.JavaSerializableUtils;

import java.io.*;
import java.util.EnumSet;

public class GraphBackup extends GraphToolBase {
    public static final String BASE64_PREFIX = "base64/java:";

    @Parameter(names = {"--out", "-o"}, description = "Output filename")
    private String outputFileName = null;

    public static void main(String[] args) throws Exception {
        GraphBackup graphBackup = new GraphBackup();
        graphBackup.run(args);
    }

    protected void run(String[] args) throws Exception {
        super.run(args);

        OutputStream out = createOutputStream();
        try {
            save(getGraph(), out, getAuthorizations());
        } finally {
            out.close();
        }
    }

    private OutputStream createOutputStream() throws FileNotFoundException {
        if (outputFileName == null) {
            return System.out;
        }
        return new FileOutputStream(outputFileName);
    }

    public void save(Graph graph, OutputStream out, Authorizations authorizations) throws IOException {
        EnumSet<FetchHint> fetchHints = FetchHint.ALL;
        save(graph.getVertices(fetchHints, authorizations), graph.getEdges(fetchHints, authorizations), out);
    }

    public void save(Iterable<Vertex> vertices, Iterable<Edge> edges, OutputStream out) throws IOException {
        saveVertices(vertices, out);
        saveEdges(edges, out);
    }

    public void saveVertices(Iterable<Vertex> vertices, OutputStream out) throws IOException {
        for (Vertex vertex : vertices) {
            saveVertex(vertex, out);
        }
    }

    public void saveVertex(Vertex vertex, OutputStream out) throws IOException {
        JSONObject json = vertexToJson(vertex);
        out.write('V');
        out.write(json.toString().getBytes());
        out.write('\n');
        saveStreamingPropertyValues(out, vertex);
    }

    public void saveEdges(Iterable<Edge> edges, OutputStream out) throws IOException {
        for (Edge edge : edges) {
            saveEdge(edge, out);
        }
    }

    public void saveEdge(Edge edge, OutputStream out) throws IOException {
        JSONObject json = edgeToJson(edge);
        out.write('E');
        out.write(json.toString().getBytes());
        out.write('\n');
        saveStreamingPropertyValues(out, edge);
    }

    private JSONObject vertexToJson(Vertex vertex) {
        return elementToJson(vertex);
    }

    private JSONObject edgeToJson(Edge edge) {
        JSONObject json = elementToJson(edge);
        json.put("outVertexId", edge.getVertexId(Direction.OUT));
        json.put("inVertexId", edge.getVertexId(Direction.IN));
        json.put("label", edge.getLabel());
        return json;
    }

    private JSONObject elementToJson(Element element) {
        JSONObject json = new JSONObject();
        json.put("id", element.getId());
        json.put("visibility", element.getVisibility().getVisibilityString());
        json.put("properties", propertiesToJson(element.getProperties()));
        return json;
    }

    private JSONArray propertiesToJson(Iterable<Property> properties) {
        JSONArray json = new JSONArray();
        for (Property property : properties) {
            if (property.getValue() instanceof StreamingPropertyValue) {
                continue;
            }
            json.put(propertyToJson(property));
        }
        return json;
    }

    private JSONObject propertyToJson(Property property) {
        JSONObject json = new JSONObject();
        json.put("key", property.getKey());
        json.put("name", property.getName());
        json.put("visibility", property.getVisibility().getVisibilityString());
        Object value = property.getValue();
        if (!(value instanceof StreamingPropertyValue)) {
            json.put("value", objectToJsonString(value));
        }
        Metadata metadata = property.getMetadata();
        if (metadata != null) {
            json.put("metadata", metadataToJson(metadata));
        }
        return json;
    }

    private JSONObject metadataToJson(Metadata metadata) {
        JSONObject json = new JSONObject();
        for (Metadata.Entry m : metadata.entrySet()) {
            json.put(m.getKey(), metadataItemToJson(m));
        }
        return json;
    }

    private JSONObject metadataItemToJson(Metadata.Entry entry) {
        JSONObject json = new JSONObject();
        json.put("value", objectToJsonString(entry.getValue()));
        json.put("visibility", entry.getVisibility().getVisibilityString());
        return json;
    }

    private void saveStreamingPropertyValues(OutputStream out, Element element) throws IOException {
        for (Property property : element.getProperties()) {
            if (property.getValue() instanceof StreamingPropertyValue) {
                saveStreamingProperty(out, property);
            }
        }
    }

    private void saveStreamingProperty(OutputStream out, Property property) throws IOException {
        StreamingPropertyValue spv = (StreamingPropertyValue) property.getValue();
        JSONObject json = propertyToJson(property);
        json.put("valueType", spv.getValueType().getName());
        json.put("searchIndex", spv.isSearchIndex());
        json.put("store", spv.isStore());
        out.write('D');
        out.write(json.toString().getBytes());
        out.write('\n');
        InputStream in = spv.getInputStream();
        byte[] buffer = new byte[10 * 1024];
        int read;
        while ((read = in.read(buffer)) > 0) {
            out.write(Integer.toString(read).getBytes());
            out.write('\n');
            out.write(buffer, 0, read);
            out.write('\n');
        }
        out.write('0');
        out.write('\n');
    }

    private String objectToJsonString(Object value) {
        if (value instanceof String) {
            return (String) value;
        }
        return BASE64_PREFIX + Base64.encodeBase64String(JavaSerializableUtils.objectToBytes(value));
    }
}
