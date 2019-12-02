package org.vertexium.accumulo.iterator.model.historicalEvents;

import com.google.protobuf.ByteSequenceByteString;
import org.apache.accumulo.core.data.ByteSequence;
import org.vertexium.accumulo.iterator.model.ElementType;
import org.vertexium.accumulo.iterator.model.proto.HistoricalDeleteEdgeEvent;
import org.vertexium.accumulo.iterator.model.proto.HistoricalEventsItem;
import org.vertexium.accumulo.iterator.util.DataInputStreamUtils;

import java.io.DataInputStream;
import java.io.IOException;

public class IteratorHistoricalDeleteEdgeEvent extends IteratorHistoricalDeleteElementEvent {
    private final ByteSequence outVertexId;
    private final ByteSequence inVertexId;
    private final ByteSequence edgeLabel;
    private final ByteSequence visibility;

    public IteratorHistoricalDeleteEdgeEvent(
        String elementId,
        ByteSequence outVertexId,
        ByteSequence inVertexId,
        ByteSequence edgeLabel,
        ByteSequence visibility,
        Long timestamp
    ) {
        super(ElementType.EDGE, elementId, timestamp);
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.edgeLabel = edgeLabel;
        this.visibility = visibility;
    }

    public ByteSequence getOutVertexId() {
        return outVertexId;
    }

    public ByteSequence getInVertexId() {
        return inVertexId;
    }

    public ByteSequence getEdgeLabel() {
        return edgeLabel;
    }

    public ByteSequence getVisibility() {
        return visibility;
    }

    static IteratorHistoricalEvent decode(DataInputStream in, String elementId, long timestamp) throws IOException {
        ByteSequence outVertexId = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence inVertexId = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence edgeLabel = DataInputStreamUtils.decodeByteSequence(in);
        ByteSequence visibility = DataInputStreamUtils.decodeByteSequence(in);
        return new IteratorHistoricalDeleteEdgeEvent(
            elementId,
            outVertexId,
            inVertexId,
            edgeLabel,
            visibility,
            timestamp
        );
    }

    @Override
    protected HistoricalEventsItem encode() {
        return HistoricalEventsItem.newBuilder()
            .setDeleteEdgeEvent(
                HistoricalDeleteEdgeEvent.newBuilder()
                    .setEvent(encodeEvent())
                    .setOutVertexId(new ByteSequenceByteString(getOutVertexId()))
                    .setInVertexId(new ByteSequenceByteString(getInVertexId()))
                    .setEdgeLabel(new ByteSequenceByteString(getEdgeLabel()))
                    .setVisibility(new ByteSequenceByteString(getVisibility()))
                    .build()
            )
            .build();
    }
}
