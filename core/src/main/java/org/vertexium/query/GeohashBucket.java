package org.vertexium.query;

import org.vertexium.type.GeoPoint;
import org.vertexium.type.GeoRect;

public abstract class GeohashBucket {
    public final String key;
    public final long count;
    private final GeoPoint geoPoint;

    public GeohashBucket(String key, long count, GeoPoint geoPoint) {
        this.key = key;
        this.count = count;
        this.geoPoint = geoPoint;
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
}
