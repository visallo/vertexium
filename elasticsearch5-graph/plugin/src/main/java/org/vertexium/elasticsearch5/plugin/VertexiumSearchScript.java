package org.vertexium.elasticsearch5.plugin;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.vertexium.elasticsearch5.VertexiumElasticsearchException;

import java.io.IOException;

public abstract class VertexiumSearchScript implements SearchScript {
    private final SearchLookup lookup;

    public VertexiumSearchScript(SearchLookup lookup) {
        this.lookup = lookup;
    }

    @Override
    public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
        LeafSearchLookup leafSearchLookup = lookup.getLeafSearchLookup(context);
        return new LeafSearchScript() {
            @Override
            public void setDocument(int doc) {
                if (leafSearchLookup == null) {
                    throw new NullPointerException();
                } else {
                    leafSearchLookup.setDocument(doc);
                }
            }

            @Override
            public double runAsDouble() {
                return VertexiumSearchScript.this.runAsDouble(leafSearchLookup);
            }

            @Override
            public long runAsLong() {
                return VertexiumSearchScript.this.runAsLong(leafSearchLookup);
            }

            @Override
            public Object run() {
                return VertexiumSearchScript.this.run(leafSearchLookup);
            }
        };
    }

    protected Object run(LeafSearchLookup leafSearchLookup) {
        throw new VertexiumElasticsearchException("not implemented: " + this.getClass().getName());
    }

    protected long runAsLong(LeafSearchLookup leafSearchLookup) {
        throw new VertexiumElasticsearchException("not implemented: " + this.getClass().getName());
    }

    protected double runAsDouble(LeafSearchLookup leafSearchLookup) {
        throw new VertexiumElasticsearchException("not implemented: " + this.getClass().getName());
    }

    @Override
    public boolean needsScores() {
        return false;
    }
}
