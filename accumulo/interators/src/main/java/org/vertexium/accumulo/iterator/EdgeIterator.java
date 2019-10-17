package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.vertexium.accumulo.iterator.model.EdgeElementData;
import org.vertexium.accumulo.iterator.model.IteratorFetchHints;
import org.vertexium.security.Authorizations;

public class EdgeIterator extends ElementIterator<EdgeElementData> {
    public static final String CF_SIGNAL_STRING = "E";
    public static final Text CF_SIGNAL = new Text(CF_SIGNAL_STRING);
    public static final byte[] CF_SIGNAL_BYTES = CF_SIGNAL.getBytes();

    public static final String CF_OUT_VERTEX_STRING = "EOUT";
    public static final Text CF_OUT_VERTEX = new Text(CF_OUT_VERTEX_STRING);
    public static final byte[] CF_OUT_VERTEX_BYTES = CF_OUT_VERTEX.getBytes();

    public static final String CF_IN_VERTEX_STRING = "EIN";
    public static final Text CF_IN_VERTEX = new Text(CF_IN_VERTEX_STRING);
    public static final byte[] CF_IN_VERTEX_BYTES = CF_IN_VERTEX.getBytes();

    public EdgeIterator() {
        this(null, (String[]) null);
    }

    public EdgeIterator(IteratorFetchHints fetchHints, String[] authorizations) {
        super(null, fetchHints, authorizations);
    }

    public EdgeIterator(SortedKeyValueIterator<Key, Value> source, IteratorFetchHints fetchHints, Authorizations authorizations) {
        super(source, fetchHints, authorizations);
    }

    public EdgeIterator(IteratorFetchHints fetchHints, Authorizations authorizations) {
        super(null, fetchHints, authorizations);
    }

    @Override
    protected boolean processColumn(KeyValue keyValue) {
        if (keyValue.columnFamilyEquals(CF_IN_VERTEX_BYTES)) {
            getElementData().inVertexId = keyValue.takeColumnQualifier();
            return true;
        }

        if (keyValue.columnFamilyEquals(CF_OUT_VERTEX_BYTES)) {
            getElementData().outVertexId = keyValue.takeColumnQualifier();
            return true;
        }

        return false;
    }

    @Override
    protected void processSignalColumn(KeyValue keyValue, boolean deleted) {
        super.processSignalColumn(keyValue, deleted);
        if (deleted) {
            getElementData().label = null;
        } else {
            getElementData().label = keyValue.takeColumnQualifier();
        }
    }

    @Override
    protected byte[] getVisibilitySignal() {
        return CF_SIGNAL_BYTES;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        if (getSourceIterator() != null) {
            return new EdgeIterator(getSourceIterator().deepCopy(env), getFetchHints(), getAuthorizations());
        }
        return new EdgeIterator(getFetchHints(), getAuthorizations());
    }

    @Override
    protected String getDescription() {
        return "This iterator encapsulates an entire Edge into a single Key/Value pair.";
    }

    @Override
    protected EdgeElementData createElementData() {
        return new EdgeElementData();
    }
}
