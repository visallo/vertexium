package org.vertexium.mutation;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.vertexium.*;
import org.vertexium.property.MutablePropertyImpl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ExistingElementMutationBaseTest {
    private TestExistingElementMutationBase mutation;

    @Mock
    private Element element;

    @Before
    public void before() {
        when(element.getFetchHints()).thenReturn(FetchHints.ALL);
        mutation = new TestExistingElementMutationBase<>(element);
    }

    @Test
    public void testEmptyMutationHasChanges() {
        assertFalse("should not have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesProperties() {
        mutation.addPropertyValue("key1", "name1", "Hello", new Visibility(""));
        assertTrue("should have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesDeleteProperty() {
        mutation.deleteProperty(new MutablePropertyImpl(
            "key1",
            "name1",
            "value",
            null,
            null,
            null,
            new Visibility(""),
            FetchHints.ALL
        ));
        assertTrue("should have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesSoftDeleteProperty() {
        mutation.softDeleteProperty(new MutablePropertyImpl(
            "key1",
            "name1",
            "value",
            null,
            null,
            null,
            new Visibility(""),
            FetchHints.ALL
        ));
        assertTrue("should have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesNewElementVisibility() {
        mutation.alterElementVisibility(new Visibility(""));
        assertTrue("should have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesAlterPropertyVisibility() {
        mutation.alterPropertyVisibility("key1", "name1", new Visibility(""));
        assertTrue("should have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesSetPropertyMetadata() {
        mutation.setPropertyMetadata("key1", "name1", "value", new Visibility(""));
        assertTrue("should have changes", mutation.hasChanges());
    }

    private static class TestExistingElementMutationBase<T extends Element> extends ExistingElementMutationBase<T> {
        public TestExistingElementMutationBase(T element) {
            super(element);
        }

        @Override
        public T save(Authorizations authorizations) {
            return null;
        }

        @Override
        public String save(User user) {
            return null;
        }
    }
}