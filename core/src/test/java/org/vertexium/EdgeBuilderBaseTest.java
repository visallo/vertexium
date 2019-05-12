package org.vertexium;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EdgeBuilderBaseTest {
    private TestEdgeBuilderBase mutation;

    @Before
    public void before() {
        mutation = new TestEdgeBuilderBase("e1", "v1", "v2", "label", new Visibility(""));
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
        protected TestEdgeBuilderBase(
            String edgeId,
            String outVertexId,
            String inVertexId,
            String label,
            Visibility visibility
        ) {
            super(edgeId, outVertexId, inVertexId, label, visibility);
        }

        @Override
        public Edge save(Authorizations authorizations) {
            return null;
        }

        @Override
        public String save(User user) {
            return null;
        }
    }
}