package org.vertexium;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RelatedEdgeSummaryImpl implements RelatedEdgeSummary {
    private Map<String, Collection<RelatedEdge>> relatedEdgesByLabel = new HashMap<>();

    public void add(String edgeId, String outVertexId, String inVertexId, String label) {
        Collection<RelatedEdge> e = relatedEdgesByLabel.get(label);
        if (e == null) {
            e = new ArrayList<>();
            relatedEdgesByLabel.put(label, e);
        }
        e.add(new RelatedEdgeImpl(edgeId, outVertexId, inVertexId));
    }

    @Override
    public Map<String, Collection<RelatedEdge>> getRelatedEdgesByLabel() {
        return relatedEdgesByLabel;
    }

    public static class RelatedEdgeImpl implements RelatedEdge {
        private final String edgeId;
        private final String inVertexId;
        private final String outVertexId;

        public RelatedEdgeImpl(String edgeId, String outVertexId, String inVertexId) {
            this.edgeId = edgeId;
            this.outVertexId = outVertexId;
            this.inVertexId = inVertexId;
        }

        @Override
        public String getEdgeId() {
            return edgeId;
        }

        @Override
        public String getInVertexId() {
            return inVertexId;
        }

        @Override
        public String getOutVertexId() {
            return outVertexId;
        }

        @Override
        public String toString() {
            return "RelatedEdgeImpl{" +
                    "edgeId='" + edgeId + '\'' +
                    ", inVertexId='" + inVertexId + '\'' +
                    ", outVertexId='" + outVertexId + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || !(o instanceof RelatedEdge)) {
                return false;
            }

            RelatedEdge that = (RelatedEdge) o;

            if (!edgeId.equals(that.getEdgeId())) {
                return false;
            }
            if (!inVertexId.equals(that.getInVertexId())) {
                return false;
            }
            if (!outVertexId.equals(that.getOutVertexId())) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return edgeId.hashCode();
        }
    }
}
