package org.vertexium.tools;

import com.beust.jcommander.Parameter;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;
import org.vertexium.*;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.util.JavaSerializableUtils;
import org.vertexium.*;

import java.io.*;

public class GraphRestore extends GraphToolBase {
    @Parameter(names = {"--in", "-i"}, description = "Input filename")
    private String inputFileName = null;

    public static void main(String[] args) throws Exception {
        GraphRestore graphRestore = new GraphRestore();
        graphRestore.run(args);
    }

    protected void run(String[] args) throws Exception {
        super.run(args);

        InputStream in = createInputStream();
        try {
            restore(getGraph(), in, getAuthorizations());
        } finally {
            in.close();
        }
    }

    private InputStream createInputStream() throws FileNotFoundException {
        if (inputFileName == null) {
            return System.in;
        }
        return new FileInputStream(inputFileName);
    }

    public void restore(Graph graph, InputStream in, Authorizations authorizations) throws IOException {
        String line;
        char lastType = 'V';
        Element element = null;
        // We can't use a BufferedReader here because when we need to read the streaming property values we need raw bytes not converted bytes
        while ((line = readLine(in)) != null) {
            try {
                char type = line.charAt(0);
                JSONObject json = new JSONObject(line.substring(1));
                switch (type) {
                    case 'V':
                        element = restoreVertex(graph, json, authorizations);
                        break;
                    case 'E':
                        // flush when we make the transition to edges so that we have vertices to lookup and link to.
                        if (type != lastType) {
                            graph.flush();
                        }
                        element = restoreEdge(graph, json, authorizations);
                        break;
                    case 'D':
                        restoreStreamingPropertyValue(in, graph, json, element, authorizations);
                        break;
                    default:
                        throw new RuntimeException("Unexpected line: " + line);
                }
                lastType = type;
            } catch (Exception ex) {
                throw new IOException("Invalid line", ex);
            }
        }
    }

    private String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        while (true) {
            int b = in.read();
            if (b < 0) {
                if (buffer.size() == 0) {
                    return null;
                }
                break;
            }
            if (b == '\n') {
                break;
            }
            buffer.write(b);
        }
        return new String(buffer.toByteArray());
    }

    private Vertex restoreVertex(Graph graph, JSONObject json, Authorizations authorizations) {
        Visibility visibility = jsonToVisibility(json);
        String vertexId = json.getString("id");
        VertexBuilder v = graph.prepareVertex(vertexId, visibility);
        jsonToProperties(json, v);
        return v.save(authorizations);
    }

    private Edge restoreEdge(Graph graph, JSONObject json, Authorizations authorizations) {
        Visibility visibility = jsonToVisibility(json);
        String edgeId = json.getString("id");
        String outVertexId = json.getString("outVertexId");
        String inVertexId = json.getString("inVertexId");
        String label = json.getString("label");
        Vertex outVertex = graph.getVertex(outVertexId, authorizations);
        Vertex inVertex = graph.getVertex(inVertexId, authorizations);
        EdgeBuilder e = graph.prepareEdge(edgeId, outVertex, inVertex, label, visibility);
        jsonToProperties(json, e);
        return e.save(authorizations);
    }

    protected Visibility jsonToVisibility(JSONObject jsonObject) {
        return new Visibility(jsonObject.getString("visibility"));
    }

    protected void jsonToProperties(JSONObject jsonObject, ElementBuilder e) {
        JSONArray propertiesJson = jsonObject.getJSONArray("properties");
        for (int i = 0; i < propertiesJson.length(); i++) {
            JSONObject propertyJson = propertiesJson.getJSONObject(i);
            jsonToProperty(propertyJson, e);
        }
    }

    private void jsonToProperty(JSONObject propertyJson, ElementBuilder e) {
        String key = propertyJson.getString("key");
        String name = propertyJson.getString("name");
        Object value = jsonStringToObject(propertyJson.getString("value"));
        Metadata metadata = jsonToPropertyMetadata(propertyJson.optJSONObject("metadata"));
        Visibility visibility = new Visibility(propertyJson.getString("visibility"));
        e.addPropertyValue(key, name, value, metadata, visibility);
    }

    private void restoreStreamingPropertyValue(InputStream in, Graph graph, JSONObject propertyJson, Element element, Authorizations authorizations) throws ClassNotFoundException, IOException {
        String key = propertyJson.getString("key");
        String name = propertyJson.getString("name");
        Metadata metadata = jsonToPropertyMetadata(propertyJson.optJSONObject("metadata"));
        Visibility visibility = new Visibility(propertyJson.getString("visibility"));
        Class valueType = Class.forName(propertyJson.getString("valueType"));
        InputStream spvin = new StreamingPropertyValueInputStream(in);
        StreamingPropertyValue value = new StreamingPropertyValue(spvin, valueType);
        value.searchIndex(propertyJson.optBoolean("searchIndex", false));
        value.store(propertyJson.optBoolean("store", true));
        element.addPropertyValue(key, name, value, metadata, visibility, authorizations);
    }

    private Metadata jsonToPropertyMetadata(JSONObject metadataJson) {
        Metadata metadata = new Metadata();
        if (metadataJson == null) {
            return metadata;
        }
        for (Object key : metadataJson.keySet()) {
            String keyString = (String) key;
            JSONObject metadataItemJson = metadataJson.getJSONObject(keyString);
            Object val = jsonStringToObject(metadataItemJson.getString("value"));
            Visibility visibility = new Visibility(metadataItemJson.getString("visibility"));
            metadata.add(keyString, val, visibility);
        }
        return metadata;
    }

    private Object jsonStringToObject(String str) {
        if (str.startsWith(GraphBackup.BASE64_PREFIX)) {
            str = str.substring(GraphBackup.BASE64_PREFIX.length());
            return JavaSerializableUtils.bytesToObject(Base64.decodeBase64(str));
        } else {
            return str;
        }
    }

    private class StreamingPropertyValueInputStream extends InputStream {
        private final InputStream in;
        private int segmentLength;
        private boolean done;

        public StreamingPropertyValueInputStream(InputStream in) throws IOException {
            this.in = in;
            readSegmentLengthLine();
        }

        private void readSegmentLengthLine() throws IOException {
            String line = readLine(this.in);
            this.segmentLength = Integer.parseInt(line);
            if (this.segmentLength == 0) {
                this.done = true;
            }
        }

        @Override
        public int read() throws IOException {
            if (this.done) {
                return -1;
            }
            if (this.segmentLength == 0) {
                this.in.read(); // throw away new line character
                readSegmentLengthLine();
                if (this.done) {
                    return -1;
                }
            }
            int ret = this.in.read();
            this.segmentLength--;
            return ret;
        }
    }
}
