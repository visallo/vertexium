package org.vertexium;

public class FindPathOptions {
    private final String sourceVertexId;
    private final String destVertexId;
    private final int maxHops;
    private String[] labels;
    private String[] excludedLabels;
    private ProgressCallback progressCallback;
    private boolean getAnyPath;

    /**
     * @param sourceVertexId The source vertex id to start the search from.
     * @param destVertexId   The destination vertex id to get to.
     * @param maxHops        The maximum number of hops to make before giving up.
     */
    public FindPathOptions(String sourceVertexId, String destVertexId, int maxHops) {
        this.sourceVertexId = sourceVertexId;
        this.destVertexId = destVertexId;
        this.maxHops = maxHops;
        this.getAnyPath = false;
    }

    /**
     * @param sourceVertexId The source vertex id to start the search from.
     * @param destVertexId   The destination vertex id to get to.
     * @param maxHops        The maximum number of hops to make before giving up.
     * @param getAnyPath     Return as soon as the first path is found
     */
    public FindPathOptions(String sourceVertexId, String destVertexId, int maxHops, boolean getAnyPath) {
        this.sourceVertexId = sourceVertexId;
        this.destVertexId = destVertexId;
        this.maxHops = maxHops;
        this.getAnyPath = getAnyPath;
    }

    public String getSourceVertexId() {
        return sourceVertexId;
    }

    public String getDestVertexId() {
        return destVertexId;
    }

    public int getMaxHops() {
        return maxHops;
    }

    public String[] getLabels() {
        return labels;
    }

    public boolean isGetAnyPath() {
        return getAnyPath;
    }

    /**
     * Edge labels to include, if null any label will be traversed
     */
    public FindPathOptions setLabels(String... labels) {
        this.labels = labels;
        return this;
    }

    public String[] getExcludedLabels() {
        return excludedLabels;
    }

    /**
     * Edge labels to be excluded from traversal
     */
    public FindPathOptions setExcludedLabels(String... excludedLabels) {
        this.excludedLabels = excludedLabels;
        return this;
    }

    public ProgressCallback getProgressCallback() {
        return progressCallback;
    }

    public FindPathOptions setProgressCallback(ProgressCallback progressCallback) {
        this.progressCallback = progressCallback;
        return this;
    }

    @Override
    public String toString() {
        return "FindPathOptions{" +
            "sourceVertexId='" + sourceVertexId + '\'' +
            ", destVertexId='" + destVertexId + '\'' +
            ", maxHops=" + maxHops +
            '}';
    }
}
