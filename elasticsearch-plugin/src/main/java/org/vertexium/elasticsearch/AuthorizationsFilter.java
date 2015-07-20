package org.vertexium.elasticsearch;

import org.apache.lucene.index.*;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;
import org.vertexium.inmemory.security.Authorizations;
import org.vertexium.inmemory.security.ColumnVisibility;
import org.vertexium.inmemory.security.VisibilityEvaluator;
import org.vertexium.inmemory.security.VisibilityParseException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AuthorizationsFilter extends Filter {
    public static String VISIBILITY_FIELD_NAME = "__visibility";
    private final VisibilityEvaluator visibilityEvaluator;
    private static final Map<BytesRef, ColumnVisibility> columnVisibilityCache = new ConcurrentHashMap<>();
    private final Authorizations authorizations;

    public AuthorizationsFilter(Authorizations authorizations) {
        this.authorizations = authorizations;
        this.visibilityEvaluator = new VisibilityEvaluator(authorizations);
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        AtomicReader reader = context.reader();
        Fields fields = reader.fields();
        Terms terms = fields.terms(VISIBILITY_FIELD_NAME);
        if (terms == null) {
            return null;
        } else {
            OpenBitSet bitSet = new OpenBitSet(reader.maxDoc());
            TermsEnum iterator = terms.iterator(null);
            BytesRef bytesRef;
            while ((bytesRef = iterator.next()) != null) {
                makeVisible(iterator, bitSet, acceptDocs, isVisible(visibilityEvaluator, bytesRef));
            }
            return BitsFilteredDocIdSet.wrap(bitSet, acceptDocs);
        }
    }

    private void makeVisible(TermsEnum iterator, OpenBitSet bitSet, Bits liveDocs, boolean visible) throws IOException {
        DocsEnum docsEnum = iterator.docs(liveDocs, null);
        int doc;
        while ((doc = docsEnum.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
            if (visible) {
                bitSet.set(doc);
            } else {
                bitSet.clear(doc);
            }
        }
    }

    private static boolean isVisible(VisibilityEvaluator visibilityEvaluator, BytesRef bytesRef) throws IOException {
        ColumnVisibility visibility = lookupColumnVisibility(bytesRef);
        if (visibility == null) {
            return true;
        }
        try {
            return visibilityEvaluator.evaluate(visibility);
        } catch (VisibilityParseException e) {
            throw new IOException(e);
        }
    }

    private static ColumnVisibility lookupColumnVisibility(BytesRef bytesRef) {
        ColumnVisibility visibility = columnVisibilityCache.get(bytesRef);
        if (visibility != null) {
            return visibility;
        }

        byte[] expression = trim(bytesRef);
        if (expression.length == 0) {
            return null;
        }
        visibility = new ColumnVisibility(expression);
        columnVisibilityCache.put(bytesRef, visibility);
        return visibility;
    }

    private static byte[] trim(BytesRef bytesRef) {
        byte[] buf = new byte[bytesRef.length];
        System.arraycopy(bytesRef.bytes, bytesRef.offset, buf, 0, bytesRef.length);
        return buf;
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }

    @Override
    public int hashCode() {
        return getAuthorizations().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AuthorizationsFilter that = (AuthorizationsFilter) o;
        return this.getAuthorizations().equals(that.getAuthorizations());
    }

    @Override
    public String toString() {
        return "AuthorizationsFilter [authorizations=" + getAuthorizations() + "]";
    }
}
