package org.vertexium.elasticsearch2;

import org.vertexium.VertexiumException;

class GeohashUtils {
    private static final int[] BITS = {16, 8, 4, 2, 1};

    public static void decodeCell(String key, org.elasticsearch.common.geo.GeoPoint northWest, org.elasticsearch.common.geo.GeoPoint southEast) {
        try {
            double[] interval = decodeCell(key);
            northWest.reset(interval[1], interval[2]);
            southEast.reset(interval[0], interval[3]);
        } catch (Exception e) {
            throw new VertexiumException("Could not decode cell", e);
        }
    }

    private static double[] decodeCell(String geohash) {
        double[] interval = {-90.0, 90.0, -180.0, 180.0};
        boolean isEven = true;

        for (int i = 0; i < geohash.length(); i++) {
            final int cd = decode(geohash.charAt(i));

            for (int mask : BITS) {
                if (isEven) {
                    if ((cd & mask) != 0) {
                        interval[2] = (interval[2] + interval[3]) / 2D;
                    } else {
                        interval[3] = (interval[2] + interval[3]) / 2D;
                    }
                } else {
                    if ((cd & mask) != 0) {
                        interval[0] = (interval[0] + interval[1]) / 2D;
                    } else {
                        interval[1] = (interval[0] + interval[1]) / 2D;
                    }
                }
                isEven = !isEven;
            }
        }
        return interval;
    }

    private static int decode(char geo) {
        switch (geo) {
            case '0':
                return 0;
            case '1':
                return 1;
            case '2':
                return 2;
            case '3':
                return 3;
            case '4':
                return 4;
            case '5':
                return 5;
            case '6':
                return 6;
            case '7':
                return 7;
            case '8':
                return 8;
            case '9':
                return 9;
            case 'b':
                return 10;
            case 'c':
                return 11;
            case 'd':
                return 12;
            case 'e':
                return 13;
            case 'f':
                return 14;
            case 'g':
                return 15;
            case 'h':
                return 16;
            case 'j':
                return 17;
            case 'k':
                return 18;
            case 'm':
                return 19;
            case 'n':
                return 20;
            case 'p':
                return 21;
            case 'q':
                return 22;
            case 'r':
                return 23;
            case 's':
                return 24;
            case 't':
                return 25;
            case 'u':
                return 26;
            case 'v':
                return 27;
            case 'w':
                return 28;
            case 'x':
                return 29;
            case 'y':
                return 30;
            case 'z':
                return 31;
            default:
                throw new VertexiumException("the character '" + geo + "' is not a valid geohash character");
        }
    }
}
