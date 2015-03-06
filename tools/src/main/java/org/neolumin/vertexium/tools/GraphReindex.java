package org.neolumin.vertexium.tools;

public class GraphReindex extends GraphToolBase {
    public static void main(String[] args) throws Exception {
        GraphReindex graphReindex = new GraphReindex();
        graphReindex.run(args);
    }

    protected void run(String[] args) throws Exception {
        super.run(args);

        System.out.println("Starting reindex");
        long startTime = System.currentTimeMillis();
        getGraph().reindex(getAuthorizations());
        long endTime = System.currentTimeMillis();
        System.out.println("Reindexing complete (" + (endTime - startTime) + "ms)");
    }
}
