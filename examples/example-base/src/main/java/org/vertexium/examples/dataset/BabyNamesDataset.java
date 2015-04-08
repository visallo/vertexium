package org.vertexium.examples.dataset;

import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.Visibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.zip.GZIPInputStream;

public class BabyNamesDataset extends Dataset {
    private static final Logger LOGGER = LoggerFactory.getLogger(BabyNamesDataset.class);

    public void load(Graph graph, int numberOfVerticesToCreate, String[] visibilities, Authorizations authorizations) throws IOException {
        LOGGER.debug("populating data count: " + numberOfVerticesToCreate);

        File file = new File("../baby-names.txt.gz");
        BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
        try {
            int i = 0;
            String line;
            while (i < numberOfVerticesToCreate && (line = br.readLine()) != null) {
                if (i % 1000 == 0) {
                    LOGGER.debug("populating data " + i + "/" + numberOfVerticesToCreate);
                }
                String[] lineParts = line.split(",");
                if (lineParts.length != 4) {
                    continue;
                }
                int year = Integer.parseInt(lineParts[0]);
                String name = lineParts[1];
                String sex = lineParts[2];
                int count = Integer.parseInt(lineParts[3]);
                Visibility visibility = new Visibility(visibilities[i % visibilities.length]);
                GregorianCalendar c = new GregorianCalendar();
                c.set(year, Calendar.JANUARY, 1, 1, 1, 1);
                c.set(Calendar.MILLISECOND, 0);
                graph.prepareVertex(visibility)
                        .setProperty("year", c.getTime(), visibility)
                        .setProperty("name", name, visibility)
                        .setProperty("sex", sex, visibility)
                        .setProperty("count", count, visibility)
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
