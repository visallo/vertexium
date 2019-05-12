package org.vertexium.accumulo.util;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.io.Text;
import org.vertexium.FetchHints;
import org.vertexium.Property;
import org.vertexium.Visibility;
import org.vertexium.accumulo.AccumuloGraph;
import org.vertexium.accumulo.LazyMutableProperty;
import org.vertexium.accumulo.MetadataEntry;
import org.vertexium.accumulo.MetadataRef;
import org.vertexium.accumulo.iterator.model.*;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;
import org.vertexium.id.NameSubstitutionStrategy;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.vertexium.accumulo.iterator.util.DataInputStreamUtils.decodeIntArray;

public class DataInputStreamUtils {
    public static Text decodeText(DataInputStream in) throws IOException {
        return org.vertexium.accumulo.iterator.util.DataInputStreamUtils.decodeText(in);
    }

    public static String decodeString(DataInputStream in) throws IOException {
        return org.vertexium.accumulo.iterator.util.DataInputStreamUtils.decodeString(in);
    }

    public static ImmutableSet<String> decodeStringSet(DataInputStream in) throws IOException {
        int count = in.readInt();
        if (count == -1) {
            return null;
        }
        ImmutableSet.Builder<String> results = ImmutableSet.builder();
        for (int i = 0; i < count; i++) {
            results.add(decodeString(in));
        }
        return results.build();
    }

    public static List<MetadataEntry> decodeMetadataEntries(DataInputStream in) throws IOException {
        int i = in.read();
        if (i != ElementData.METADATA_START) {
            throw new IOException(String.format("Unexpected metadata start: 0x%02x (expected: 0x%02x)", i, ElementData.METADATA_START));
        }

        int length = in.readInt();
        List<MetadataEntry> results = new ArrayList<>(length);
        for (i = 0; i < length; i++) {
            results.add(decodeMetadataEntry(in));
        }

        i = in.read();
        if (i != ElementData.METADATA_END) {
            throw new IOException(String.format("Unexpected metadata end: 0x%02x (expected: 0x%02x)", i, ElementData.METADATA_END));
        }

        return results;
    }

    private static MetadataEntry decodeMetadataEntry(DataInputStream in) throws IOException {
        String metadataKey = decodeString(in);
        String metadataVisibility = decodeString(in);
        int valueLength = in.readInt();
        byte[] value = new byte[valueLength];
        in.read(value);
        return new MetadataEntry(metadataKey, metadataVisibility, value);
    }

    public static Iterable<Property> decodeProperties(
        AccumuloGraph graph,
        DataInputStream in,
        List<MetadataEntry> metadataEntries,
        FetchHints fetchHints
    ) throws IOException {
        List<Property> results = new ArrayList<>();
        while (true) {
            int propId = in.read();
            if (propId == ElementData.PROP_END) {
                break;
            } else if (propId != ElementData.PROP_START) {
                throw new IOException("Unexpected prop id: " + propId);
            }
            String propertyKey = graph.getNameSubstitutionStrategy().inflate(decodeString(in));
            String propertyName = graph.getNameSubstitutionStrategy().inflate(decodeString(in));
            Visibility propertyVisibility = new Visibility(decodeString(in));
            long propertyTimestamp = in.readLong();
            int propertyValueLength = in.readInt();
            byte[] propertyValue = new byte[propertyValueLength];
            int read = in.read(propertyValue);
            if (read != propertyValueLength) {
                throw new IOException("Unexpected data length expected " + propertyValueLength + " found " + read);
            }
            Set<String> propertyHiddenVisibilitiesStringSet = decodeStringSet(in);
            Set<Visibility> propertyHiddenVisibilities = null;
            if (propertyHiddenVisibilitiesStringSet != null) {
                propertyHiddenVisibilities = propertyHiddenVisibilitiesStringSet.stream()
                    .map(Visibility::new)
                    .collect(Collectors.toSet());
            }
            MetadataRef metadataRef = decodePropertyMetadata(in, metadataEntries);
            results.add(new LazyMutableProperty(
                graph,
                graph.getVertexiumSerializer(),
                propertyKey,
                propertyName,
                propertyValue,
                metadataRef,
                propertyHiddenVisibilities,
                propertyVisibility,
                propertyTimestamp,
                fetchHints
            ));
        }
        return results;
    }

    private static MetadataRef decodePropertyMetadata(
        DataInputStream in,
        List<MetadataEntry> metadataEntries
    ) throws IOException {
        int[] metadataIndexes = decodeIntArray(in);
        return new MetadataRef(
            metadataEntries,
            metadataIndexes
        );
    }

    public static Edges decodeEdges(DataInputStream in, NameSubstitutionStrategy nameSubstitutionStrategy) throws IOException {
        int edgeLabelMarker = in.readByte();
        if (edgeLabelMarker == DataOutputStreamUtils.EDGE_LABEL_WITH_REFS_MARKER) {
            return decodeEdgesWithRefs(in, nameSubstitutionStrategy);
        } else if (edgeLabelMarker == DataOutputStreamUtils.EDGE_LABEL_ONLY_MARKER) {
            return decodeEdgesLabelsOnly(in, nameSubstitutionStrategy);
        } else {
            throw new IOException("Unexpected edge label marker: " + edgeLabelMarker);
        }
    }

    private static Edges decodeEdgesLabelsOnly(DataInputStream in, NameSubstitutionStrategy nameSubstitutionStrategy) throws IOException {
        EdgesWithCount edges = new EdgesWithCount();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String label = nameSubstitutionStrategy.inflate(decodeString(in));
            int edgeByLabelCount = in.readInt();
            edges.add(label, edgeByLabelCount);
        }
        return edges;
    }

    private static Edges decodeEdgesWithRefs(DataInputStream in, NameSubstitutionStrategy nameSubstitutionStrategy) throws IOException {
        EdgesWithEdgeInfo edges = new EdgesWithEdgeInfo();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String label = decodeString(in);
            int edgeByLabelCount = in.readInt();
            for (int edgeByLabelIndex = 0; edgeByLabelIndex < edgeByLabelCount; edgeByLabelIndex++) {
                Text edgeId = decodeText(in);
                long timestamp = in.readLong();
                String vertexId = decodeString(in);
                Text columnVisibility = decodeText(in);
                EdgeInfo edgeInfo = new EdgeInfo(
                    nameSubstitutionStrategy.inflate(label),
                    vertexId,
                    columnVisibility,
                    timestamp
                );
                edges.add(edgeId, edgeInfo);
            }
        }
        return edges;
    }

    public static void decodeHeader(DataInputStream in, byte expectedTypeId) throws IOException {
        byte[] header = new byte[ElementData.HEADER.length];
        int read = in.read(header);
        if (read != header.length) {
            throw new IOException("Unexpected header length. Expected " + ElementData.HEADER.length + " found " + read);
        }
        if (!Arrays.equals(header, ElementData.HEADER)) {
            throw new IOException("Unexpected header");
        }
        int typeId = in.read();
        if (typeId != expectedTypeId) {
            throw new IOException("Unexpected type id. Expected " + expectedTypeId + " found " + typeId);
        }
    }
}
