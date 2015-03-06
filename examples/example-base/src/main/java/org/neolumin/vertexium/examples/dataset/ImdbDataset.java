package org.neolumin.vertexium.examples.dataset;

import org.neolumin.vertexium.Authorizations;
import org.neolumin.vertexium.Graph;
import org.neolumin.vertexium.Vertex;
import org.neolumin.vertexium.Visibility;

import java.io.*;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

public class ImdbDataset extends Dataset {
    public static final String CATEGORY_ID_PREFIX = "CATEGORY_";
    public static final Object MOVIE_ID_PREFIX = "MOVIE_";

    public void load(Graph graph, int numberOfVerticesToCreate, String[] visibilities, Authorizations authorizations) throws FileNotFoundException {
        InputStream in = new FileInputStream(new File("../imdb.csv"));
        Map<String, Vertex> categoryCache = new HashMap<String, Vertex>();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            for (int i = 0; i < numberOfVerticesToCreate; i++) {
                String line = reader.readLine();
                if (line == null) {
                    throw new RuntimeException("Not enough lines in file. Needed " + numberOfVerticesToCreate + " found " + i);
                }
                String[] parts = line.split("\t");
                String title = parts[0];
                int year = Integer.parseInt(parts[1]);
                double rating = Double.parseDouble(parts[2]);
                String[] categoriesArray;
                if (parts.length < 5) {
                    categoriesArray = new String[0];
                } else {
                    categoriesArray = parts[4].split(",");
                }

                GregorianCalendar c = new GregorianCalendar();
                c.set(year, Calendar.JANUARY, 1, 1, 1, 1);
                c.set(Calendar.MILLISECOND, 0);
                Visibility visibility = new Visibility(visibilities[i % visibilities.length]);
                Vertex movieVertex = graph.prepareVertex(MOVIE_ID_PREFIX + title, visibility)
                        .setProperty("title", title, visibility)
                        .setProperty("year", c.getTime(), visibility)
                        .setProperty("rating", rating, visibility)
                        .save(authorizations);
                for (String category : categoriesArray) {
                    visibility = new Visibility("");
                    Vertex categoryVertex = categoryCache.get(category);
                    if (categoryVertex == null) {
                        categoryVertex = graph.prepareVertex(CATEGORY_ID_PREFIX + category, visibility)
                                .setProperty("title", category, visibility)
                                .save(authorizations);
                        categoryCache.put(category, categoryVertex);
                    }
                    graph.addEdge(categoryVertex.getId() + "->" + movieVertex.getId(), categoryVertex, movieVertex, "hasMovie", visibility, authorizations);
                }
            }
            graph.flush();
        } catch (Exception e) {
            throw new RuntimeException("Could not create vertices", e);
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                // do nothing
            }
        }
    }
}
