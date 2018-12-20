package org.vertexium.mutation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.vertexium.Authorizations;
import org.vertexium.Edge;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExistingEdgeMutationTest {
    private TestExistingEdgeMutation mutation;

    @BeforeEach
    public void before() {
        mutation = new TestExistingEdgeMutation(null);
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

    private static class TestExistingEdgeMutation extends ExistingEdgeMutation {
        public TestExistingEdgeMutation(Edge edge) {
            super(edge);
        }

        @Override
        public Edge save(Authorizations authorizations) {
            return null;
        }
    }
}