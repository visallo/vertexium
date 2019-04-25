package org.vertexium.accumulo.iterator.model.historicalEvents;

import org.apache.accumulo.core.data.ByteSequence;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;
import org.vertexium.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IteratorHistoricalAlterEdgeLabelEvent extends IteratorHistoricalEvent {
    private final ByteSequence edgeLabel;

    public IteratorHistoricalAlterEdgeLabelEvent(
        String elementId,
        ByteSequence edgeLabel,
        Long timestamp
    ) {
        super(ElementType.EDGE, elementId, timestamp);
        this.edgeLabel = edgeLabel;
    }

    public ByteSequence getEdgeLabel() {
        return edgeLabel;
    }

    static IteratorHistoricalEvent decode(DataInputStream in, String elementId, long timestamp) throws IOException {
        ByteSequence edgeLabel = DataInputStreamUtils.decodeByteSequence(in);
        return new IteratorHistoricalAlterEdgeLabelEvent(
            elementId,
            edgeLabel,
            timestamp
        );
    }

    @Override
    protected void encode(DataOutputStream out) throws IOException {
        super.encode(out);
        DataOutputStreamUtils.encodeByteSequence(out, getEdgeLabel());
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_ALTER_EDGE_LABEL;
    }
}
