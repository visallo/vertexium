package org.vertexium.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.vertexium.Element;
import org.vertexium.VertexiumException;
import org.vertexium.query.*;
import org.vertexium.type.GeoPoint;
import org.vertexium.type.GeoRect;

import java.util.HashMap;
import java.util.Map;

public class ElasticSearchGraphQueryIterable<T extends Element> extends DefaultGraphQueryIterable<T> implements
        IterableWithTotalHits<T>,
        IterableWithSearchTime<T>,
        IterableWithScores<T>,
        IterableWithHistogramResults<T>,
        IterableWithTermsResults<T>,
        IterableWithGeohashResults<T> {
    private final ElasticSearchQueryBase query;
    private final SearchResponse searchResponse;
    private final long totalHits;
    private final long searchTimeInNanoSeconds;
    private final Map<String, Double> scores = new HashMap<>();

    public ElasticSearchGraphQueryIterable(
            ElasticSearchQueryBase query,
            SearchResponse searchResponse,
            QueryParameters parameters,
            Iterable<T> iterable,
            boolean evaluateQueryString,
            boolean evaluateHasContainers,
            boolean evaluateSortContainers,
            long totalHits,
            long searchTimeInNanoSeconds,
            SearchHits hits
    ) {
        super(parameters, iterable, evaluateQueryString, evaluateHasContainers, evaluateSortContainers);
        this.query = query;
        this.searchResponse = searchResponse;
        this.totalHits = totalHits;
        this.searchTimeInNanoSeconds = searchTimeInNanoSeconds;
        if (hits != null) {
            for (SearchHit hit : hits.getHits()) {
                scores.put(hit.getId(), (double) hit.getScore());
            }
        }
    }

    @Override
    public long getTotalHits() {
        return this.totalHits;
    }

    @Override
    public Map<String, Double> getScores() {
        return this.scores;
    }

    @Override
    public long getSearchTimeNanoSeconds() {
        return this.searchTimeInNanoSeconds;
    }

    @Override
    public HistogramResult getHistogramResults(String name) {
        Map<Object, HistogramBucket> buckets = new HashMap<>();
        for (Aggregation agg : this.searchResponse.getAggregations()) {
            String aggName = query.getAggregationName(agg.getName());
            if (!aggName.equals(name)) {
                continue;
            }

            if (agg instanceof DateHistogram) {
                DateHistogram h = (DateHistogram) agg;
                for (DateHistogram.Bucket b : h.getBuckets()) {
                    HistogramBucket existingBucket = buckets.get(b.getKey());
                    long existingCount = 0;
                    if (existingBucket != null) {
                        existingCount = existingBucket.getCount();
                    }
                    buckets.put(b.getKeyAsDate().toDate(), new HistogramBucket(b.getKeyAsDate().toDate(), existingCount + b.getDocCount()));
                }
            } else if (agg instanceof Histogram) {
                Histogram h = (Histogram) agg;
                for (Histogram.Bucket b : h.getBuckets()) {
                    HistogramBucket existingBucket = buckets.get(b.getKey());
                    long existingCount = 0;
                    if (existingBucket != null) {
                        existingCount = existingBucket.getCount();
                    }
                    buckets.put(b.getKey(), new HistogramBucket(b.getKey(), existingCount + b.getDocCount()));
                }
            } else {
                throw new VertexiumException("Aggregation is not a histogram: " + agg.getClass().getName());
            }
        }
        return new HistogramResult(buckets.values());
    }

    @Override
    public TermsResult getTermsResults(String name) {
        Map<String, TermsBucket> buckets = new HashMap<>();
        for (Aggregation agg : this.searchResponse.getAggregations()) {
            String aggName = query.getAggregationName(agg.getName());
            if (!aggName.equals(name)) {
                continue;
            }
            if (agg instanceof Terms) {
                Terms h = (Terms) agg;
                for (Terms.Bucket b : h.getBuckets()) {
                    String mapKey = b.getKey().toLowerCase();
                    TermsBucket existingBucket = buckets.get(mapKey);
                    long existingCount = 0;
                    if (existingBucket != null) {
                        existingCount = existingBucket.getCount();
                    }
                    buckets.put(mapKey, new TermsBucket(mapKey, existingCount + b.getDocCount()));
                }
            } else {
                throw new VertexiumException("Aggregation is not a terms: " + agg.getClass().getName());
            }
        }
        return new TermsResult(buckets.values());
    }

    @Override
    public GeohashResult getGeohashResults(String name) {
        Map<String, GeohashBucket> buckets = new HashMap<>();
        for (Aggregation agg : this.searchResponse.getAggregations()) {
            String aggName = query.getAggregationName(agg.getName());
            if (!aggName.equals(name)) {
                continue;
            }
            if (agg instanceof GeoHashGrid) {
                GeoHashGrid h = (GeoHashGrid) agg;
                for (GeoHashGrid.Bucket b : h.getBuckets()) {
                    org.elasticsearch.common.geo.GeoPoint g = b.getKeyAsGeoPoint();
                    GeohashBucket existingBucket = buckets.get(b.getKey());
                    long existingCount = 0;
                    if (existingBucket != null) {
                        existingCount = existingBucket.getCount();
                    }
                    GeohashBucket geohashBucket = new GeohashBucket(b.getKey(), existingCount + b.getDocCount(), new GeoPoint(g.getLat(), g.getLon())) {
                        @Override
                        public GeoRect getGeoCell() {
                            org.elasticsearch.common.geo.GeoPoint northWest = new org.elasticsearch.common.geo.GeoPoint();
                            org.elasticsearch.common.geo.GeoPoint southEast = new org.elasticsearch.common.geo.GeoPoint();
                            GeohashUtils.decodeCell(getKey(), northWest, southEast);
                            return new GeoRect(new GeoPoint(northWest.getLat(), northWest.getLon()), new GeoPoint(southEast.getLat(), southEast.getLon()));
                        }
                    };
                    buckets.put(b.getKey(), geohashBucket);
                }
            } else {
                throw new VertexiumException("Aggregation is not a geohash: " + agg.getClass().getName());
            }
        }
        return new GeohashResult(buckets.values());
    }
}
