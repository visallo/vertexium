package org.neolumin.vertexium.examples.dataset;

import org.neolumin.vertexium.Authorizations;
import org.neolumin.vertexium.Graph;
import org.neolumin.vertexium.Visibility;
import org.neolumin.vertexium.type.GeoPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class GeoNamesDataset extends Dataset {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeoNamesDataset.class);

    public void load(Graph graph, int numberOfVerticesToCreate, String[] visibilities, Authorizations authorizations) throws IOException {
        LOGGER.debug("populating data count: " + numberOfVerticesToCreate);

        File file = new File("../geonames-cities15000.txt.gz");
        BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
        try {
            int i = 0;
            String line;
            while (i < numberOfVerticesToCreate && (line = br.readLine()) != null) {
                if (i % 1000 == 0) {
                    LOGGER.debug("populating data " + i + "/" + numberOfVerticesToCreate);
                }
                String[] lineParts = line.split("\t");
                if (lineParts.length < 15) {
                    continue;
                }
                int geoNamesId = Integer.parseInt(lineParts[0]);
                String name = lineParts[1];
                String asciiName = lineParts[2];
                String alternateNames = lineParts[3];
                Double latitude = Double.parseDouble(lineParts[4]);
                Double longitude = Double.parseDouble(lineParts[5]);
                Long population = Long.parseLong(lineParts[14]);
                Visibility visibility = new Visibility(visibilities[i % visibilities.length]);
                graph.prepareVertex("" + geoNamesId, visibility)
                        .setProperty("name", name, visibility)
                        .setProperty("asciiName", asciiName, visibility)
                        .setProperty("alternateNames", alternateNames, visibility)
                        .setProperty("population", population, visibility)
                        .setProperty("geoLocation", new GeoPoint(latitude, longitude), visibility)
                        .save(authorizations);
                i++;
            }
        } finally {
            br.close();
        }
        graph.flush();
        LOGGER.debug("populated data");
    }
}
