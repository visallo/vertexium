package org.vertexium.benchmark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.io.IOUtils;
import org.vertexium.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.vertexium.util.Preconditions.checkNotNull;

public class VertexiumBenchmark {
    @Parameter(names = {"--help", "-h"}, description = "Print help", help = true)
    private boolean help;

    @Parameter(names = {"-c"}, description = "Configuration file name", required = true)
    private String configFileName = null;

    @Parameter(names = {"-cp"}, description = "Configuration property prefix")
    private String configPropertyPrefix = null;

    private String[] words;
    private final Random random = new Random(1);
    private static final long RANDOM_START_TIME = 1429553899717L;

    public static void main(String[] args) throws IOException {
        int result = new VertexiumBenchmark().run(args);
        System.exit(result);
    }

    private int run(String[] args) throws IOException {
        JCommander j = new JCommander(this, args);
        if (help) {
            j.usage();
            return -1;
        }

        this.words = readWords();

        Map config = loadConfig();
        config.put("tableNamePrefix", "vertexium_benchmark_");
        config.put("search.indexName", "vertexium_benchmark");
        Graph graph = new GraphFactory().createGraph(config);
        Authorizations authorizations = graph.createAuthorizations();
        Map<Integer, Integer> timeResultsByBatchSize = runBenchmark(graph, authorizations);
        printResults(timeResultsByBatchSize);
        graph.shutdown();
        return 0;
    }

    private Map<Integer, Integer> runBenchmark(Graph graph, Authorizations authorizations) {
        Map<Integer, Integer> timeResultsByBatchSize = new HashMap<>();
        for (int batchSize = 10; batchSize <= 1000; batchSize = batchSize * 10) {
            int vertexCount = batchSize * 25;
            int verticesPerSecond = runBenchmark(graph, vertexCount, batchSize, authorizations);
            timeResultsByBatchSize.put(batchSize, verticesPerSecond);
        }
        return timeResultsByBatchSize;
    }

    private int runBenchmark(Graph graph, int vertexCount, int batchSize, Authorizations authorizations) {
        graph.truncate();
        Visibility visibility = new Visibility("");
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < vertexCount; i++) {
            if ((i % batchSize) == 0) {
                System.out.println("Saving (batch size " + batchSize + "): " + i + "/" + vertexCount);
                graph.flush();
            }
            VertexBuilder v = graph.prepareVertex(visibility);
            String value = generateRandomText(1000);
            Metadata metadata = new Metadata();
            metadata.add("createdBy", generateRandomText(2), visibility);
            metadata.add("createdDate", generateRandomDate(), visibility);
            v.addPropertyValue("ket1", "name1", value, metadata, visibility);
            v.save(authorizations);
        }
        graph.flush();
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println(String.format("Results for batch size %d: %ds %dv/s", batchSize, (totalTime / 1000), (vertexCount / (totalTime / 1000))));
        return (int) ((double) vertexCount / ((double) totalTime / 1000.0));
    }

    private void printResults(Map<Integer, Integer> timeResultsByBatchSize) {
        System.out.println("Results:");
        System.out.println("--------------------------------------------------------------------");
        List<Map.Entry<Integer, Integer>> entries = new ArrayList<>(timeResultsByBatchSize.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<Integer, Integer>>() {
            @Override
            public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                return Integer.compare(o1.getKey(), o2.getKey());
            }
        });
        for (Map.Entry<Integer, Integer> entry : entries) {
            System.out.println(String.format("%-5d %dv/s", entry.getKey(), entry.getValue()));
        }
    }

    private String[] readWords() throws IOException {
        try (InputStream wordsIn = this.getClass().getResourceAsStream("words.txt")) {
            checkNotNull(wordsIn, "Could not find words.txt");
            List<String> words = IOUtils.readLines(wordsIn);
            return words.toArray(new String[words.size()]);
        }
    }

    private Date generateRandomDate() {
        return new Date(RANDOM_START_TIME + (long) random.nextInt(10000));
    }

    private String generateRandomText(int wordCount) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < wordCount; i++) {
            if (i > 0) {
                result.append(' ');
            }
            result.append(words[random.nextInt(words.length)]);
        }
        return result.toString();
    }

    private Map loadConfig() throws IOException {
        File configFile = new File(configFileName);
        if (!configFile.exists()) {
            throw new RuntimeException("Could not load config file: " + configFile.getAbsolutePath());
        }

        Properties props = new Properties();
        try (InputStream in = new FileInputStream(configFile)) {
            props.load(in);
        }

        Map result = new HashMap();
        if (configPropertyPrefix == null) {
            result.putAll(props);
        } else {
            for (Map.Entry<Object, Object> p : props.entrySet()) {
                String key = (String) p.getKey();
                String val = (String) p.getValue();
                if (key.startsWith(configPropertyPrefix + ".")) {
                    result.put(key.substring((configPropertyPrefix + ".").length()), val);
                } else if (key.startsWith(configPropertyPrefix)) {
                    result.put(key.substring(configPropertyPrefix.length()), val);
                }
            }
        }

        return result;
    }
}
