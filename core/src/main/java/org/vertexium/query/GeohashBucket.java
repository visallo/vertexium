package org.vertexium.query;

import org.vertexium.type.GeoPoint;
import org.vertexium.type.GeoRect;

import java.util.Map;

public abstract class GeohashBucket {
    private final String key;
    private final long count;
    private final GeoPoint geoPoint;
    private final Map<String, AggregationResult> nestedResults;

    public GeohashBucket(String key, long count, GeoPoint geoPoint, Map<String, AggregationResult> nestedResults) {
        this.key = key;
        this.count = count;
        this.geoPoint = geoPoint;
        this.nestedResults = nestedResults;
    }

    public String getKey() {
        return key;
    }

    public long getCount() {
        return count;
    }

    public GeoPoint getGeoPoint() {
        return geoPoint;
    }

    public abstract GeoRect getGeoCell();

    public Map<String, AggregationResult> getNestedResults() {
        return nestedResults;
    }
}
