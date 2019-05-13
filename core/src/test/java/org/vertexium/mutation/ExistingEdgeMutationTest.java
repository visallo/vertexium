package org.vertexium.mutation;

import org.junit.Before;
import org.junit.Test;
import org.vertexium.Authorizations;
import org.vertexium.Edge;
import org.vertexium.User;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExistingEdgeMutationTest {
    private TestExistingEdgeMutation mutation;

    @Before
    public void before() {
        mutation = new TestExistingEdgeMutation(null);
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

    private static class TestExistingEdgeMutation extends ExistingEdgeMutation {
        public TestExistingEdgeMutation(Edge edge) {
            super(edge);
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