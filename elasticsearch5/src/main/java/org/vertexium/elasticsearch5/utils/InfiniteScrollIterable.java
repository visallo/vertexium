package org.vertexium.elasticsearch5.utils;

import org.elasticsearch.action.search.SearchResponse;
import org.vertexium.query.AggregationResult;
import org.vertexium.query.QueryResultsIterable;
import org.vertexium.util.CloseableIterator;
import org.vertexium.util.CloseableUtils;

import java.util.ArrayList;
import java.util.Iterator;

public abstract class InfiniteScrollIterable<T> implements QueryResultsIterable<T> {
    private SearchResponse firstResponse;
    private QueryResultsIterable<T> firstIterable;
    private boolean initCalled;
    private boolean firstCall;

    protected abstract SearchResponse getInitialSearchResponse();

    protected abstract SearchResponse getNextSearchResponse(String scrollId);

    protected abstract QueryResultsIterable<T> searchResponseToIterable(SearchResponse searchResponse);

    protected abstract void closeScroll(String scrollId);

    @Override
    public void close() {
    }

    private void init() {
        if (initCalled) {
            return;
        }
        firstResponse = getInitialSearchResponse();
        if (firstResponse == null) {
            firstIterable = null;
        } else {
            firstIterable = searchResponseToIterable(firstResponse);
        }
        firstCall = true;
        initCalled = true;
    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        init();
        return firstIterable.getAggregationResult(name, resultType);
    }

    @Override
    public long getTotalHits() {
        init();
        if (firstIterable == null) {
            return 0;
        }
        return firstIterable.getTotalHits();
    }

    @Override
    public Iterator<T> iterator() {
        init();
        if (firstResponse == null) {
            return new ArrayList<T>().iterator();
        }

        SearchResponse response;
        Iterator<T> it;
        if (firstCall) {
            response = firstResponse;
            it = firstIterable.iterator();
            firstCall = false;
        } else {
            response = getInitialSearchResponse();
            it = searchResponseToIterable(response).iterator();
        }
        String scrollId = response.getScrollId();
        return new InfiniteIterator(scrollId, it);
    }

    private class InfiniteIterator implements CloseableIterator<T> {
        private final String scrollId;
        private Iterator<T> it;
        private T next;
        private T current;

        public InfiniteIterator(String scrollId, Iterator<T> it) {
            this.scrollId = scrollId;
            this.it = it;
        }

        @Override
        public boolean hasNext() {
            loadNext();
            if (next == null) {
                close();
            }
            return next != null;
        }

        @Override
        public T next() {
            loadNext();
            this.current = this.next;
            this.next = null;
            return this.current;
        }

        private void loadNext() {
            if (this.next != null) {
                return;
            }

            if (it.hasNext()) {
                this.next = it.next();
            } else {
                CloseableUtils.closeQuietly(it);
                QueryResultsIterable<T> iterable = searchResponseToIterable(getNextSearchResponse(scrollId));
                it = iterable.iterator();
                if (!it.hasNext()) {
                    it = null;
                } else {
                    this.next = it.next();
                }
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closeScroll(scrollId);
            CloseableUtils.closeQuietly(it);
            InfiniteScrollIterable.this.close();
        }
    }
}
