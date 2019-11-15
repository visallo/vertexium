package org.vertexium.elasticsearch7.utils;

import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch7.ElasticsearchGraphQueryIterable;
import org.vertexium.query.AggregationResult;
import org.vertexium.query.IterableWithScores;
import org.vertexium.query.IterableWithTotalHits;
import org.vertexium.query.QueryResultsIterable;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class PagingIterable<T> implements
    Iterable<T>,
    IterableWithTotalHits<T>,
    IterableWithScores<T>,
    QueryResultsIterable<T> {
    private final long skip;
    private final long limit;
    private boolean isFirstCallToIterator;
    private final ElasticsearchGraphQueryIterable<T> firstIterable;
    private final int pageSize;

    public PagingIterable(long skip, Long limit, int pageSize) {
        this.skip = skip;
        this.limit = limit == null ? Long.MAX_VALUE : limit;
        this.pageSize = pageSize;

        // This is a bit of a hack. Because the underlying iterable is the iterable with geohash results, histogram results, etc.
        //   we need to grab the first iterable to get the results out.
        long firstIterableLimit = Math.min(pageSize, this.limit);
        this.firstIterable = getPageIterable((int) this.skip, (int) firstIterableLimit, true);
        this.isFirstCallToIterator = true;
    }

    @Override
    public Double getScore(Object id) {
        return this.firstIterable.getScore(id);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        return this.firstIterable.getAggregationResult(name, resultType);
    }

    @Override
    public long getTotalHits() {
        return this.firstIterable.getTotalHits();
    }

    protected abstract ElasticsearchGraphQueryIterable<T> getPageIterable(int skip, int limit, boolean includeAggregations);

    @Override
    public Iterator<T> iterator() {
        MyIterator it = new MyIterator(isFirstCallToIterator ? firstIterable : null);
        isFirstCallToIterator = false;
        return it;
    }

    private class MyIterator implements Iterator<T> {
        private ElasticsearchGraphQueryIterable<T> firstIterable;
        private long currentResultNumber = 0;
        private long lastIterableResultNumber = 0;
        private long lastPageSize = 0;
        private Iterator<T> currentIterator;

        public MyIterator(ElasticsearchGraphQueryIterable<T> firstIterable) {
            this.firstIterable = firstIterable;
            this.currentResultNumber = skip;
            this.currentIterator = getNextIterator();
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (currentIterator == null) {
                    currentIterator = getNextIterator();
                    if (currentIterator == null) {
                        return false;
                    }
                }
                if (currentIterator.hasNext()) {
                    return true;
                }
                currentIterator = null;
            }
        }

        @Override
        public T next() {
            if (hasNext()) {
                currentResultNumber++;
                return currentIterator.next();
            }
            throw new NoSuchElementException();
        }

        private Iterator<T> getNextIterator() {
            long totalReturned = currentResultNumber - skip;
            long lastIterableCount = currentResultNumber - lastIterableResultNumber;
            if (totalReturned >= limit || currentResultNumber >= getTotalHits() || lastIterableCount < lastPageSize) {
                return null;
            }
            long nextPageSize = lastPageSize = Math.min(pageSize, limit - currentResultNumber);
            if (firstIterable == null) {
                if (nextPageSize <= 0) {
                    return null;
                }
                firstIterable = getPageIterable((int) currentResultNumber, (int) nextPageSize, false);
            }
            Iterator<T> it = firstIterable.iterator();
            firstIterable = null;
            lastIterableResultNumber = currentResultNumber;
            return it;
        }

        @Override
        public void remove() {
            throw new VertexiumException("remove not implemented");
        }
    }
}
