package org.vertexium.cli;

import com.beust.jcommander.Parameter;
import com.google.common.collect.Lists;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.VertexiumException;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ParametersBase {
    @Parameter(names = {"--help", "-h"}, description = "Print help", help = true)
    public boolean help;

    @Parameter(names = {"-c"}, description = "Configuration file name")
    public List<String> configFileNames = new ArrayList<>();

    @Parameter(names = {"-cd"}, description = "Configuration directories (all files ending in .properties)")
    public List<String> configDirectories = new ArrayList<>();

    @Parameter(names = {"-cp"}, description = "Configuration property prefix")
    public String configPropertyPrefix = null;

    @Parameter(names = {"-a"}, description = "Authorizations")
    public String authorizations = null;

    @Parameter(names = {"--cypherLabelProperty"}, description = "Cypher label property")
    public String cypherLabelProperty = null;

    private static List<String> addConfigDirectoryToConfigFileNames(String configDirectory) {
        File dir = new File(configDirectory);
        if (!dir.exists()) {
            throw new VertexiumException("Directory does not exist: " + dir.getAbsolutePath());
        }
        List<String> files = Lists.newArrayList(dir.listFiles()).stream()
                .filter(File::isFile)
                .map(File::getName)
                .filter(f -> f.endsWith(".properties"))
                .collect(Collectors.toList());
        Collections.sort(files);
        files = files.stream()
                .map(f -> new File(dir, f).getAbsolutePath())
                .collect(Collectors.toList());
        return files;
    }

    public List<String> getConfigFileNames() {
        List<String> results = new ArrayList<>(configFileNames);
        for (String configDirectory : configDirectories) {
            results.addAll(addConfigDirectoryToConfigFileNames(configDirectory));
        }
        return results;
    }

    public Authorizations getAuthorizations(Graph graph) {
        return graph.createAuthorizations(authorizations == null ? null : authorizations.split(","));
    }
}
