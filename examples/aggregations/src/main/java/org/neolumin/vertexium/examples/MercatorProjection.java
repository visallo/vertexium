package org.neolumin.vertexium.examples;

public class MercatorProjection {
    public static double longitudeToX(double longitude, int width) {
        return (longitude + 180.0) / 360.0 * (double) width;
    }

    public static double latitudeToY(double latitude, int height) {
        latitude = latitude * 1.2;
        double sinLatitude = Math.sin(latitude * (Math.PI / 180.0));
        double pixelY = (0.5 - Math.log((1 + sinLatitude) / (1.0 - sinLatitude)) / (4.0 * Math.PI)) * (double) height;
        return Math.min(Math.max(0, pixelY), height);
    }
}
