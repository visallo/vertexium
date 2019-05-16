package org.vertexium.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.GraphFactory;
import org.vertexium.cypher.VertexiumCypherQuery;
import org.vertexium.cypher.VertexiumCypherQueryContext;
import org.vertexium.cypher.VertexiumCypherResult;
import org.vertexium.cypher.ast.CypherCompilerContext;
import org.vertexium.util.ConfigurationUtils;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExecuteCypherQuery {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(ExecuteCypherQuery.class);

    private static class Parameters extends ParametersBase {
        @Parameter(description = "Cypher query to run", required = true)
        private List<String> query;
    }

    public static void main(String[] args) throws Exception {
        int result = new ExecuteCypherQuery().run(args);
        System.exit(result);
    }

    private int run(String[] args) throws Exception {
        Parameters params = new Parameters();
        JCommander j = new JCommander(params, args);
        if (params.help) {
            j.usage();
            return -1;
        }

        Map<String, String> config = ConfigurationUtils.loadConfig(params.getConfigFileNames(), params.configPropertyPrefix);
        Graph graph = new GraphFactory().createGraph(config);

        Authorizations authorizations = params.getAuthorizations(graph);
        VertexiumCypherQueryContext ctx = new CliVertexiumCypherQueryContext(graph, authorizations.getUser());
        CliVertexiumCypherQueryContext.setLabelPropertyName(params.cypherLabelProperty);
        CypherCompilerContext compilerContext = new CypherCompilerContext(ctx.getFunctions());

        long startTime = System.currentTimeMillis();
        String queryString = params.query.stream().collect(Collectors.joining(" "));
        VertexiumCypherQuery query = VertexiumCypherQuery.parse(compilerContext, queryString);
        VertexiumCypherResult results = query.execute(ctx);
        results.stream().forEach(row -> {
            results.getColumnNames().forEach(column -> {
                System.out.println(row.getByName(column));
            });
        });
        long endTime = System.currentTimeMillis();
        LOGGER.info("total time: %.2fs", ((double) (endTime - startTime) / 1000.0));

        return 0;
    }
}
