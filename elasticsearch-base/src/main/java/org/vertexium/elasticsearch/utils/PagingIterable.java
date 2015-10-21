package org.vertexium.elasticsearch.utils;

import org.vertexium.Element;
import org.vertexium.VertexiumException;
import org.vertexium.elasticsearch.ElasticSearchGraphQueryIterable;
import org.vertexium.query.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public abstract class PagingIterable<T extends Element> implements
        Iterable<T>,
        IterableWithTotalHits<T>,
        IterableWithScores<T>,
        IterableWithHistogramResults<T>,
        IterableWithTermsResults<T>,
        IterableWithGeohashResults<T>,
        IterableWithStatisticsResults<T>,
        QueryResultsIterable<T> {
    private static final int PAGE_SIZE = 1000;
    private final long skip;
    private final Long limit;
    private boolean isFirstCallToIterator;
    private final ElasticSearchGraphQueryIterable<T> firstIterable;

    public PagingIterable(long skip, Long limit) {
        this.skip = skip;
        this.limit = limit;

        // This is a bit of a hack. Because the underlying iterable is the iterable with geohash results, histogram results, etc.
        //   we need to grab the first iterable to get the results out.
        int firstIterableLimit = Math.min(PAGE_SIZE, limit == null ? Integer.MAX_VALUE : limit.intValue());
        this.firstIterable = getPageIterable((int) this.skip, firstIterableLimit);
        this.isFirstCallToIterator = true;
    }

    @Override
    public GeohashResult getGeohashResults(String name) {
        return this.firstIterable.getGeohashResults(name);
    }

    @Override
    public HistogramResult getHistogramResults(String name) {
        return this.firstIterable.getHistogramResults(name);
    }

    @Override
    public StatisticsResult getStatisticsResults(String name) {
        return this.firstIterable.getStatisticsResults(name);
    }

    @Override
    public Map<String, Double> getScores() {
        return this.firstIterable.getScores();
    }

    @Override
    public TermsResult getTermsResults(String name) {
        return this.firstIterable.getTermsResults(name);
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

    protected abstract ElasticSearchGraphQueryIterable<T> getPageIterable(int skip, int limit);

    @Override
    public Iterator<T> iterator() {
        MyIterator it = new MyIterator(isFirstCallToIterator ? firstIterable : null);
        isFirstCallToIterator = false;
        return it;
    }

    private class MyIterator implements Iterator<T> {
        private ElasticSearchGraphQueryIterable<T> firstIterable;
        private int nextSkip;
        private int limit;
        private int currentIteratorCount;
        private Iterator<T> currentIterator;

        public MyIterator(ElasticSearchGraphQueryIterable<T> firstIterable) {
            this.firstIterable = firstIterable;
            this.nextSkip = (int) skip;
            this.limit = (int) (PagingIterable.this.limit == null ? Integer.MAX_VALUE : PagingIterable.this.limit);
            this.currentIterator = getNextIterator();
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (currentIterator == null) {
                    if (currentIteratorCount == 0) {
                        return false;
                    }
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
                limit--;
                currentIteratorCount++;
                return currentIterator.next();
            }
            throw new NoSuchElementException();
        }

        private Iterator<T> getNextIterator() {
            if (limit <= 0) {
                return null;
            }
            int limit = Math.min(PAGE_SIZE, this.limit);
            currentIteratorCount = 0;
            if (firstIterable == null) {
                firstIterable = getPageIterable(nextSkip, limit);
            }
            Iterator<T> it = firstIterable.iterator();
            firstIterable = null;
            nextSkip += limit;
            return it;
        }

        @Override
        public void remove() {
            throw new VertexiumException("remove not implemented");
        }
    }
}
