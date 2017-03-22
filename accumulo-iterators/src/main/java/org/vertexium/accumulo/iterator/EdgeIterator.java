package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.EdgeElementData;
import org.vertexium.accumulo.iterator.model.IteratorFetchHint;

import java.util.EnumSet;

public class EdgeIterator extends ElementIterator<EdgeElementData> {
    public static final String CF_SIGNAL_STRING = "E";
    public static final Text CF_SIGNAL = new Text(CF_SIGNAL_STRING);
    public static final String CF_OUT_VERTEX_STRING = "EOUT";
    public static final Text CF_OUT_VERTEX = new Text(CF_OUT_VERTEX_STRING);
    public static final String CF_IN_VERTEX_STRING = "EIN";
    public static final Text CF_IN_VERTEX = new Text(CF_IN_VERTEX_STRING);

    public EdgeIterator() {
        this(IteratorFetchHint.ALL);
    }

    public EdgeIterator(EnumSet<IteratorFetchHint> fetchHints) {
        super(null, fetchHints);
    }

    public EdgeIterator(SortedKeyValueIterator<Key, Value> source, EnumSet<IteratorFetchHint> fetchHints) {
        super(source, fetchHints);
    }

    @Override
    protected boolean processColumn(Key key, Value value, Text columnFamily, Text columnQualifier) {
        if (CF_IN_VERTEX.compareTo(columnFamily) == 0) {
            getElementData().inVertexId = key.getColumnQualifier();
            return true;
        }

        if (CF_OUT_VERTEX.compareTo(columnFamily) == 0) {
            getElementData().outVertexId = key.getColumnQualifier();
            return true;
        }

        return false;
    }

    @Override
    protected void processSignalColumn(Text columnQualifier) {
        super.processSignalColumn(columnQualifier);
        getElementData().label = columnQualifier;
    }

    @Override
    protected Text getVisibilitySignal() {
        return CF_SIGNAL;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        if (sourceIter != null) {
            return new EdgeIterator(sourceIter.deepCopy(env), getFetchHints());
        }
        return new EdgeIterator(getFetchHints());
    }

    @Override
    protected EdgeElementData createElementData() {
        return new EdgeElementData();
    }
}
