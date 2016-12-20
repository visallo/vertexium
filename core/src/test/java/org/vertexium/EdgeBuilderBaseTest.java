package org.vertexium;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EdgeBuilderBaseTest {
    private TestEdgeBuilderBase mutation;

    @Before
    public void before() {
        mutation = new TestEdgeBuilderBase("e1", "label", new Visibility(""));
    }

    @Test
    public void testEmptyMutationHasChanges() {
        assertFalse("should not have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesAlterEdgeLabel() {
        mutation.alterEdgeLabel("newEdgeLabel");
        assertTrue("should have changes", mutation.hasChanges());
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