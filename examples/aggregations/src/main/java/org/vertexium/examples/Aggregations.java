package org.vertexium.examples;

import com.v5analytics.webster.App;
import com.v5analytics.webster.HandlerChain;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertexium.Authorizations;
import org.vertexium.Vertex;
import org.vertexium.query.*;
import org.vertexium.type.GeoPoint;
import org.vertexium.type.GeoRect;

import javax.imageio.ImageIO;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

public class Aggregations extends ExampleBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(Aggregations.class);
    private static Aggregations _this;

    public static void main(String[] args) throws Exception {
        LOGGER.debug("begin " + Aggregations.class.getName());
        _this = new Aggregations();
        _this.run(args);
    }

    @Override
    protected Class<? extends Servlet> getServletClass() {
        return Router.class;
    }

    public static class Router extends RouterBase {
        @Override
        protected void initApp(ServletConfig config, App app) {
            super.initApp(config, app);

            app.get("/search", Search.class);
            app.get("/worldmap.png", WorldMap.class);
        }
    }

    public static enum QueryType {
        HISTOGRAM,
        TERMS,
        GEOHASH
    }

    public static class Search extends HandlerBase {

        @Override
        public void handle(HttpServletRequest request, HttpServletResponse response, HandlerChain handlerChain) throws Exception {
            String authorizationsString = getRequiredParameter(request, "authorizations");
            Authorizations authorizations = createAuthorizations(authorizationsString.split(","));

            String q = getRequiredParameter(request, "q");
            String field = getRequiredParameter(request, "field");
            String interval = getRequiredParameter(request, "interval");
            int precision = Integer.parseInt(getRequiredParameter(request, "precision"));
            QueryType queryType = getQueryType(request, "querytype");

            String AGGREGATION_NAME = "agg";

            Iterable<Vertex> vertices = queryForVertices(AGGREGATION_NAME, q, field, interval, precision, queryType, authorizations);

            JSONObject json = new JSONObject();
            if (vertices instanceof IterableWithTotalHits) {
                json.put("totalHits", ((IterableWithTotalHits) vertices).getTotalHits());
            }
            if (queryType == QueryType.HISTOGRAM) {
                json.put("histogramResult", histogramResultToJson(getHistogramResult(vertices, AGGREGATION_NAME)));
            } else if (queryType == QueryType.TERMS) {
                json.put("termsResult", termsResultToJson(getTermsResult(vertices, AGGREGATION_NAME)));
            } else if (queryType == QueryType.GEOHASH) {
                json.put("geohashResult", geohashResultToJson(getGeohashResult(vertices, AGGREGATION_NAME)));
            } else {
                throw new RuntimeException("Unsupported queryType: " + queryType);
            }

            response.getOutputStream().write(json.toString(2).getBytes());
        }

        private static HistogramResult getHistogramResult(Iterable<Vertex> vertices, String aggregationName) {
            if (!(vertices instanceof IterableWithHistogramResults)) {
                throw new RuntimeException("query results " + vertices.getClass().getName() + " does not support histograms");
            }
            return ((IterableWithHistogramResults) vertices).getHistogramResults(aggregationName);
        }

        private JSONObject histogramResultToJson(HistogramResult histogramResult) {
            JSONObject json = new JSONObject();

            JSONArray bucketsJson = new JSONArray();
            for (HistogramBucket bucket : histogramResult.getBuckets()) {
                JSONObject bucketJson = new JSONObject();
                Object key = bucket.getKey();
                if (key instanceof Date) {
                    key = ((Date) key).getTime();
                }
                bucketJson.put("key", key);
                bucketJson.put("count", bucket.getCount());
                bucketsJson.put(bucketJson);
            }
            json.put("buckets", bucketsJson);

            return json;
        }

        private static TermsResult getTermsResult(Iterable<Vertex> vertices, String aggregationName) {
            if (!(vertices instanceof IterableWithTermsResults)) {
                throw new RuntimeException("query results " + vertices.getClass().getName() + " does not support terms");
            }
            return ((IterableWithTermsResults) vertices).getTermsResults(aggregationName);
        }

        private JSONObject termsResultToJson(TermsResult termsResult) {
            JSONObject json = new JSONObject();

            JSONArray bucketsJson = new JSONArray();
            for (TermsBucket bucket : termsResult.getBuckets()) {
                JSONObject bucketJson = new JSONObject();
                Object key = bucket.getKey();
                if (key instanceof Date) {
                    key = ((Date) key).getTime();
                }
                bucketJson.put("key", key);
                bucketJson.put("count", bucket.getCount());
                bucketsJson.put(bucketJson);
            }
            json.put("buckets", bucketsJson);

            return json;
        }

        private static GeohashResult getGeohashResult(Iterable<Vertex> vertices, String aggregationName) {
            if (!(vertices instanceof IterableWithGeohashResults)) {
                throw new RuntimeException("query results " + vertices.getClass().getName() + " does not support geohash");
            }
            return ((IterableWithGeohashResults) vertices).getGeohashResults(aggregationName);
        }

        private JSONObject geohashResultToJson(GeohashResult geohashResult) {
            JSONObject json = new JSONObject();

            JSONArray bucketsJson = new JSONArray();
            for (GeohashBucket bucket : geohashResult.getBuckets()) {
                JSONObject bucketJson = new JSONObject();
                String key = bucket.getKey();
                bucketJson.put("key", key);
                bucketJson.put("count", bucket.getCount());
                bucketJson.put("geoPoint", geoPointToJson(bucket.getGeoPoint()));
                bucketsJson.put(bucketJson);
            }
            json.put("buckets", bucketsJson);

            return json;
        }

        private JSONObject geoPointToJson(GeoPoint geoPoint) {
            JSONObject json = new JSONObject();
            json.put("latitude", geoPoint.getLatitude());
            json.put("longitude", geoPoint.getLongitude());
            json.put("altitude", geoPoint.getAltitude());
            json.put("description", geoPoint.getDescription());
            return json;
        }
    }

    public static class WorldMap extends HandlerBase {
        @Override
        public void handle(HttpServletRequest request, HttpServletResponse response, HandlerChain chain) throws Exception {
            String authorizationsString = getRequiredParameter(request, "authorizations");
            Authorizations authorizations = createAuthorizations(authorizationsString.split(","));

            String q = getRequiredParameter(request, "q");
            String field = getRequiredParameter(request, "field");
            int precision = Integer.parseInt(getRequiredParameter(request, "precision"));

            String AGGREGATION_NAME = "agg";
            Iterable<Vertex> vertices = queryForVertices(AGGREGATION_NAME, q, field, null, precision, QueryType.GEOHASH, authorizations);
            if (vertices instanceof IterableWithGeohashResults) {
                GeohashResult geohashResults = ((IterableWithGeohashResults) vertices).getGeohashResults(AGGREGATION_NAME);
                RenderedImage img = createMap(geohashResults, precision);
                response.setHeader("content-type", "image/png");
                ImageIO.write(img, "png", response.getOutputStream());
            } else {
                throw new RuntimeException("Invalid results " + vertices.getClass().getName() + ". Does not implement " + IterableWithGeohashResults.class.getName());
            }
        }

        private RenderedImage createMap(GeohashResult geohashResults, int precision) throws IOException {
            try (InputStream worldmapGifIn = this.getClass().getResourceAsStream("/worldmap.png")) {
                BufferedImage img = ImageIO.read(worldmapGifIn);
                double maxCount = geohashResults.getMaxCount();

                Graphics g = img.getGraphics();
                try {
                    for (GeohashBucket b : geohashResults.getBuckets()) {
                        int value = (int) (255.0 * ((double) b.getCount() / maxCount));
                        Color c = new Color(255, 0, 0, value);
                        g.setColor(c);
                        GeoRect geoCell = b.getGeoCell();
                        int x1 = (int) MercatorProjection.longitudeToX(geoCell.getNorthWest().getLongitude(), img.getWidth());
                        int y1 = (int) MercatorProjection.latitudeToY(geoCell.getNorthWest().getLatitude(), img.getHeight());
                        int x2 = (int) MercatorProjection.longitudeToX(geoCell.getSouthEast().getLongitude(), img.getWidth());
                        int y2 = (int) MercatorProjection.latitudeToY(geoCell.getSouthEast().getLatitude(), img.getHeight());
                        g.fillRect(x1, y1, x2 - x1, y2 - y1);
                    }
                } finally {
                    g.dispose();
                }
                return img;
            }
        }
    }

    protected static QueryType getQueryType(HttpServletRequest request, String parameterName) {
        String queryType = HandlerBase.getRequiredParameter(request, parameterName);
        return QueryType.valueOf(queryType);
    }

    protected static Iterable<Vertex> queryForVertices(String histogramName, String q, String field, String interval, int precision, QueryType queryType, Authorizations authorizations) {
        Query query = _this.getGraph()
                .query(q, authorizations)
                .limit(0);
        if (queryType == QueryType.TERMS && query instanceof GraphQueryWithTermsAggregation) {
            ((GraphQueryWithTermsAggregation) query).addTermsAggregation(histogramName, field);
        } else if (queryType == QueryType.HISTOGRAM && query instanceof GraphQueryWithHistogramAggregation) {
            ((GraphQueryWithHistogramAggregation) query).addHistogramAggregation(histogramName, field, interval);
        } else if (queryType == QueryType.GEOHASH && query instanceof GraphQueryWithGeohashAggregation) {
            ((GraphQueryWithGeohashAggregation) query).addGeohashAggregation(histogramName, field, precision);
        } else {
            throw new RuntimeException("query " + query.getClass().getName() + " does not support " + queryType);
        }
        return query.vertices();
    }
}
