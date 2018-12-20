package org.vertexium;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EdgeBuilderBaseTest {
    private TestEdgeBuilderBase mutation;

    @BeforeEach
    public void before() {
        mutation = new TestEdgeBuilderBase("e1", "label", new Visibility(""));
    }

    @Test
    public void testEmptyMutationHasChanges() {
        assertFalse(mutation.hasChanges(), "should not have changes");
    }

    @Test
    public void testHasChangesAlterEdgeLabel() {
        mutation.alterEdgeLabel("newEdgeLabel");
        assertTrue(mutation.hasChanges(), "should have changes");
    }

    private static class TestEdgeBuilderBase extends EdgeBuilderBase {
        protected TestEdgeBuilderBase(String edgeId, String label, Visibility visibility) {
            super(edgeId, label, visibility);
        }

        @Override
        public Edge save(Authorizations authorizations) {
            return null;
        }

        @Override
        public String getOutVertexId() {
            return null;
        }

        @Override
        public String getInVertexId() {
            return null;
        }
    }
}