package org.vertexium.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.vertexium.Element;
import org.vertexium.VertexiumException;
import org.vertexium.query.*;
import org.vertexium.type.GeoPoint;
import org.vertexium.type.GeoRect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("deprecation")
public class ElasticSearchGraphQueryIterable<T extends Element> extends DefaultGraphQueryIterable<T> implements
        IterableWithTotalHits<T>,
        IterableWithSearchTime<T>,
        IterableWithScores<T>,
        IterableWithHistogramResults<T>,
        IterableWithTermsResults<T>,
        IterableWithGeohashResults<T>,
        IterableWithStatisticsResults<T> {
    private final long totalHits;
    private final long searchTimeInNanoSeconds;
    private final Map<String, Double> scores = new HashMap<>();
    private final Map<String, AggregationResult> aggregationResults;

    public ElasticSearchGraphQueryIterable(
            ElasticSearchSingleDocumentSearchQueryBase query,
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
        ElasticSearchSingleDocumentSearchQueryBase query1 = query;
        SearchResponse searchResponse1 = searchResponse;
        this.totalHits = totalHits;
        this.searchTimeInNanoSeconds = searchTimeInNanoSeconds;
        if (hits != null) {
            for (SearchHit hit : hits.getHits()) {
                scores.put(hit.getId(), (double) hit.getScore());
            }
        }
        this.aggregationResults = getAggregationResults(query1, searchResponse1);
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
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        AggregationResult result = this.aggregationResults.get(name);
        if (result == null) {
            return AggregationResult.createEmptyResult(resultType);
        }
        if (!resultType.isInstance(result)) {
            throw new VertexiumException("Could not cast aggregation result of type " + result.getClass().getName() + " to type " + resultType.getName());
        }
        return resultType.cast(result);
    }

    private static Map<String, AggregationResult> getAggregationResults(ElasticSearchSingleDocumentSearchQueryBase query, SearchResponse searchResponse) {
        if (searchResponse == null) {
            return new HashMap<>();
        }
        Map<String, List<Aggregation>> aggsByName = getAggregationResultsByName(query, searchResponse.getAggregations());
        return reduceAggregationResults(query, aggsByName);
    }

    private static Map<String, List<Aggregation>> getAggregationResultsByName(ElasticSearchSingleDocumentSearchQueryBase query, Iterable<Aggregation> aggs) {
        Map<String, List<Aggregation>> aggsByName = new HashMap<>();
        if (aggs == null) {
            return aggsByName;
        }
        for (Aggregation agg : aggs) {
            String aggName = query.getAggregationName(agg.getName());
            List<Aggregation> l = aggsByName.get(aggName);
            if (l == null) {
                l = new ArrayList<>();
                aggsByName.put(aggName, l);
            }
            l.add(agg);
        }
        return aggsByName;
    }

    private static Map<String, AggregationResult> reduceAggregationResults(ElasticSearchSingleDocumentSearchQueryBase query, Map<String, List<Aggregation>> aggsByName) {
        Map<String, AggregationResult> results = new HashMap<>();
        for (Map.Entry<String, List<Aggregation>> entry : aggsByName.entrySet()) {
            results.put(entry.getKey(), reduceAggregationResults(query, entry.getValue()));
        }
        return results;
    }

    private static AggregationResult reduceAggregationResults(ElasticSearchSingleDocumentSearchQueryBase query, List<Aggregation> aggs) {
        if (aggs.size() == 0) {
            throw new VertexiumException("Cannot reduce zero sized aggregation list");
        }
        Aggregation first = aggs.get(0);
        if (first instanceof HistogramAggregation || first instanceof InternalHistogram) {
            return reduceHistogramResults(query, aggs);
        }
        if (first instanceof TermsAggregation || first instanceof InternalTerms) {
            return reduceTermsResults(query, aggs);
        }
        if (first instanceof GeohashAggregation || first instanceof InternalGeoHashGrid) {
            return reduceGeohashResults(query, aggs);
        }
        if (first instanceof StatisticsAggregation || first instanceof InternalExtendedStats) {
            return reduceStatisticsResults(aggs);
        }
        throw new VertexiumException("Unhandled aggregation type: " + first.getClass().getName());
    }

    private static HistogramResult reduceHistogramResults(ElasticSearchSingleDocumentSearchQueryBase query, List<Aggregation> aggs) {
        Map<Object, List<MultiBucketsAggregation.Bucket>> bucketsByKey = new HashMap<>();
        for (Aggregation agg : aggs) {
            if (agg instanceof Histogram) {
                Histogram h = (Histogram) agg;
                org.vertexium.query.Aggregation queryAgg = query.getAggregationByName(query.getAggregationName(h.getName()));
                boolean isCalendarFieldQuery = queryAgg != null && queryAgg instanceof CalendarFieldAggregation;
                for (Histogram.Bucket b : h.getBuckets()) {
                    if (isCalendarFieldQuery && b.getKey().equals(-1L)) {
                        continue;
                    }
                    List<MultiBucketsAggregation.Bucket> l = bucketsByKey.get(b.getKey());
                    if (l == null) {
                        l = new ArrayList<>();
                        bucketsByKey.put(b.getKey(), l);
                    }
                    l.add(b);
                }
            } else {
                throw new VertexiumException("Aggregation is not a histogram: " + agg.getClass().getName());
            }
        }
        return new MultiBucketsAggregationReducer<HistogramResult, HistogramBucket>() {
            @Override
            protected HistogramBucket createBucket(Object key, long count, Map<String, AggregationResult> nestedResults, List<MultiBucketsAggregation.Bucket> buckets) {
                return new HistogramBucket(key, count, nestedResults);
            }

            @Override
            protected HistogramResult bucketsToResults(List<HistogramBucket> buckets) {
                return new HistogramResult(buckets);
            }
        }.reduce(query, bucketsByKey);
    }

    private static TermsResult reduceTermsResults(ElasticSearchSingleDocumentSearchQueryBase query, List<Aggregation> aggs) {
        Map<Object, List<MultiBucketsAggregation.Bucket>> bucketsByKey = new HashMap<>();
        for (Aggregation agg : aggs) {
            if (agg instanceof Terms) {
                Terms h = (Terms) agg;
                for (Terms.Bucket b : h.getBuckets()) {
                    String mapKey = bucketKeyToString(b.getKey());
                    List<MultiBucketsAggregation.Bucket> existingBucketByName = bucketsByKey.get(mapKey);
                    if (existingBucketByName == null) {
                        existingBucketByName = new ArrayList<>();
                        bucketsByKey.put(mapKey, existingBucketByName);
                    }
                    existingBucketByName.add(b);
                }
            } else {
                throw new VertexiumException("Aggregation is not a terms: " + agg.getClass().getName());
            }
        }
        return new MultiBucketsAggregationReducer<TermsResult, TermsBucket>() {
            @Override
            protected TermsBucket createBucket(Object key, long count, Map<String, AggregationResult> nestedResults, List<MultiBucketsAggregation.Bucket> buckets) {
                return new TermsBucket(key, count, nestedResults);
            }

            @Override
            protected TermsResult bucketsToResults(List<TermsBucket> buckets) {
                return new TermsResult(buckets);
            }
        }.reduce(query, bucketsByKey);
    }

    private abstract static class MultiBucketsAggregationReducer<TResult, TBucket> {
        public TResult reduce(ElasticSearchSingleDocumentSearchQueryBase query, Map<Object, List<MultiBucketsAggregation.Bucket>> bucketsByKey) {
            List<TBucket> buckets = new ArrayList<>();
            for (Map.Entry<Object, List<MultiBucketsAggregation.Bucket>> bucketsByKeyEntry : bucketsByKey.entrySet()) {
                String key = bucketKeyToString(bucketsByKeyEntry.getKey());
                long count = 0;
                List<Aggregation> subAggs = new ArrayList<>();
                for (MultiBucketsAggregation.Bucket b : bucketsByKeyEntry.getValue()) {
                    count += b.getDocCount();
                    for (Aggregation subAgg : b.getAggregations()) {
                        subAggs.add(subAgg);
                    }
                }
                Map<String, AggregationResult> nestedResults = reduceAggregationResults(query, getAggregationResultsByName(query, subAggs));
                buckets.add(createBucket(key, count, nestedResults, bucketsByKeyEntry.getValue()));
            }
            return bucketsToResults(buckets);
        }

        protected abstract TBucket createBucket(Object key, long count, Map<String, AggregationResult> nestedResults, List<MultiBucketsAggregation.Bucket> buckets);

        protected abstract TResult bucketsToResults(List<TBucket> buckets);
    }

    private static String bucketKeyToString(Object bucketKey) {
        if (bucketKey instanceof org.elasticsearch.common.geo.GeoPoint) {
            String geohash = ((org.elasticsearch.common.geo.GeoPoint) bucketKey).getGeohash();
            return geohash.replaceAll("0+$", "");
        }
        return bucketKey.toString();
    }

    private static GeohashResult reduceGeohashResults(ElasticSearchSingleDocumentSearchQueryBase query, List<Aggregation> aggs) {
        Map<Object, List<MultiBucketsAggregation.Bucket>> bucketsByKey = new HashMap<>();
        for (Aggregation agg : aggs) {
            if (agg instanceof GeoHashGrid) {
                GeoHashGrid h = (GeoHashGrid) agg;
                for (GeoHashGrid.Bucket b : h.getBuckets()) {
                    List<MultiBucketsAggregation.Bucket> existingBucket = bucketsByKey.get(b.getKey());
                    if (existingBucket == null) {
                        existingBucket = new ArrayList<>();
                        bucketsByKey.put(b.getKey(), existingBucket);
                    }
                    existingBucket.add(b);
                }
            } else {
                throw new VertexiumException("Aggregation is not a geohash: " + agg.getClass().getName());
            }
        }
        return new MultiBucketsAggregationReducer<GeohashResult, GeohashBucket>() {
            @Override
            protected GeohashBucket createBucket(final Object key, long count, Map<String, AggregationResult> nestedResults, List<MultiBucketsAggregation.Bucket> buckets) {
                GeoPoint geoPoint = getAverageGeoPointFromBuckets(buckets);
                return new GeohashBucket(key.toString(), count, geoPoint, nestedResults) {
                    @Override
                    public GeoRect getGeoCell() {
                        org.elasticsearch.common.geo.GeoPoint northWest = new org.elasticsearch.common.geo.GeoPoint();
                        org.elasticsearch.common.geo.GeoPoint southEast = new org.elasticsearch.common.geo.GeoPoint();
                        GeohashUtils.decodeCell(key.toString(), northWest, southEast);
                        return new GeoRect(new GeoPoint(northWest.getLat(), northWest.getLon()), new GeoPoint(southEast.getLat(), southEast.getLon()));
                    }
                };
            }

            @Override
            protected GeohashResult bucketsToResults(List<GeohashBucket> buckets) {
                return new GeohashResult(buckets);
            }
        }.reduce(query, bucketsByKey);
    }

    private static GeoPoint getAverageGeoPointFromBuckets(List<MultiBucketsAggregation.Bucket> buckets) {
        List<GeoPoint> geoPoints = new ArrayList<>();
        for (MultiBucketsAggregation.Bucket b : buckets) {
            GeoHashGrid.Bucket gb = (GeoHashGrid.Bucket) b;
            org.elasticsearch.common.geo.GeoPoint gp = (org.elasticsearch.common.geo.GeoPoint) gb.getKey();
            geoPoints.add(new GeoPoint(gp.getLat(), gp.getLon()));
        }
        return GeoPoint.calculateCenter(geoPoints);
    }

    private static StatisticsResult reduceStatisticsResults(List<Aggregation> aggs) {
        List<StatisticsResult> results = new ArrayList<>();
        for (Aggregation agg : aggs) {
            if (agg instanceof ExtendedStats) {
                ExtendedStats extendedStats = (ExtendedStats) agg;
                long count = extendedStats.getCount();
                double sum = extendedStats.getSum();
                double min = extendedStats.getMin();
                double max = extendedStats.getMax();
                double standardDeviation = extendedStats.getStdDeviation();
                results.add(new StatisticsResult(count, sum, min, max, standardDeviation));
            } else {
                throw new VertexiumException("Aggregation is not a statistics: " + agg.getClass().getName());
            }
        }
        return StatisticsResult.combine(results);
    }

    @SuppressWarnings("deprecation")
    @Override
    public GeohashResult getGeohashResults(String name) {
        return this.getAggregationResult(name, GeohashResult.class);
    }

    @SuppressWarnings("deprecation")
    @Override
    public HistogramResult getHistogramResults(String name) {
        return this.getAggregationResult(name, HistogramResult.class);
    }

    @SuppressWarnings("deprecation")
    @Override
    public StatisticsResult getStatisticsResults(String name) {
        return this.getAggregationResult(name, StatisticsResult.class);
    }

    @SuppressWarnings("deprecation")
    @Override
    public TermsResult getTermsResults(String name) {
        return this.getAggregationResult(name, TermsResult.class);
    }
}
