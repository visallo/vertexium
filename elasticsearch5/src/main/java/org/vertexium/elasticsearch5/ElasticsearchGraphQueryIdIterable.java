package org.vertexium.elasticsearch5;

import org.elasticsearch.search.SearchHit;
import org.vertexium.query.AggregationResult;
import org.vertexium.query.QueryResultsIterable;
import org.vertexium.util.CloseableUtils;
import org.vertexium.util.ConvertingIterable;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.io.IOException;

public class ElasticsearchGraphQueryIdIterable<T>
        extends ConvertingIterable<SearchHit, T>
        implements QueryResultsIterable<T> {

    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ElasticsearchGraphQueryIdIterable.class);

    private final IdStrategy idStrategy;
    private final QueryResultsIterable<SearchHit> iterable;

    public ElasticsearchGraphQueryIdIterable(IdStrategy idStrategy, QueryResultsIterable<SearchHit> iterable) {
        super(iterable);
        this.idStrategy = idStrategy;
        this.iterable = iterable;
    }

    @Override
    public void close() throws IOException {
        CloseableUtils.closeQuietly(iterable);
    }

    @Override
    public long getTotalHits() {
        return iterable.getTotalHits();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T convert(SearchHit hit) {
        ElasticsearchDocumentType dt = ElasticsearchDocumentType.fromSearchHit(hit);
        T convertedId = null;
        if (dt != null) {
            switch (dt) {
                case VERTEX:
                    convertedId = (T) idStrategy.vertexIdFromSearchHit(hit);
                    break;
                case EDGE:
                    convertedId = (T) idStrategy.edgeIdFromSearchHit(hit);
                    break;
                case VERTEX_EXTENDED_DATA:
                case EDGE_EXTENDED_DATA:
                    convertedId = (T) idStrategy.extendedDataRowIdFromSearchHit(hit);
                    break;
                default:
                    LOGGER.warn("Unhandled document type: %s", dt);
                    break;
            }
        }
        return convertedId;
    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        return iterable.getAggregationResult(name, resultType);
    }
}
