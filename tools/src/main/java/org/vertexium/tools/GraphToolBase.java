package org.vertexium.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.GraphFactory;
import org.vertexium.accumulo.AccumuloAuthorizations;
import org.vertexium.util.MapUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

@Parameters(separators = "=")
public abstract class GraphToolBase {
    @Parameter(names = {"-c", "--config"}, description = "Configuration file name")
    private String configFileName = null;

    @Parameter(names = {"--configPrefix"}, description = "Prefix of graph related configuration parameters")
    private String configPrefix = null;

    @Parameter(names = {"-a", "--auth"}, description = "Comma separated string of Authorizations")
    private String authString = "";

    private Graph graph;

    protected void run(String[] args) throws Exception {
        new JCommander(this, args);

        if (configFileName == null) {
            throw new RuntimeException("config is required");
        }

        Map config = new Properties();
        InputStream in = new FileInputStream(configFileName);
        try {
            ((Properties) config).load(in);
        } finally {
            in.close();
        }
        if (configPrefix != null) {
            config = MapUtils.getAllWithPrefix(config, configPrefix);
        }
        graph = new GraphFactory().createGraph(config);
    }

    protected Authorizations getAuthorizations() {
        // TODO change this to be configurable
        String[] split = authString.split(",");
        if (split.length == 1 && split[0].length() == 0) {
            split = new String[0];
        }
        return new AccumuloAuthorizations(split);
    }

    protected Graph getGraph() {
        return graph;
    }
}
