package org.vertexium;

import org.junit.Test;

import static org.junit.Assert.fail;

public class FetchHintsTest {
    @Test
    public void hasFetchHints_ignoreAdditionalVisibilities() {
        assertHasFetchHints(
            new FetchHintsBuilder().setIgnoreAdditionalVisibilities(true).build(),
            new FetchHintsBuilder().setIgnoreAdditionalVisibilities(true).build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIgnoreAdditionalVisibilities(true).build(),
            new FetchHintsBuilder().setIgnoreAdditionalVisibilities(false).build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIgnoreAdditionalVisibilities(false).build(),
            new FetchHintsBuilder().setIgnoreAdditionalVisibilities(true).build()
        );
    }

    @Test
    public void hasFetchHints_includeHidden() {
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeHidden(true).build(),
            new FetchHintsBuilder().setIncludeHidden(true).build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIncludeHidden(true).build(),
            new FetchHintsBuilder().setIncludeHidden(false).build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIncludeHidden(false).build(),
            new FetchHintsBuilder().setIncludeHidden(true).build()
        );
    }

    @Test
    public void hasFetchHints_includeEdgeLabelsAndCounts() {
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeEdgeLabelsAndCounts(true).build(),
            new FetchHintsBuilder().setIncludeEdgeLabelsAndCounts(true).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeEdgeLabelsAndCounts(true).build(),
            new FetchHintsBuilder().setIncludeEdgeLabelsAndCounts(false).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeEdgeLabelsAndCounts(false).build(),
            new FetchHintsBuilder().setIncludeEdgeLabelsAndCounts(false).build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIncludeEdgeLabelsAndCounts(false).build(),
            new FetchHintsBuilder().setIncludeEdgeLabelsAndCounts(true).build()
        );
    }

    @Test
    public void hasFetchHints_includeAllEdgeRefs() {
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllEdgeRefs(true).build(),
            new FetchHintsBuilder().setIncludeAllEdgeRefs(true).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllEdgeRefs(true).build(),
            new FetchHintsBuilder().setIncludeAllEdgeRefs(false).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllEdgeRefs(false).build(),
            new FetchHintsBuilder().setIncludeAllEdgeRefs(false).build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIncludeAllEdgeRefs(false).build(),
            new FetchHintsBuilder().setIncludeAllEdgeRefs(true).build()
        );
    }

    @Test
    public void hasFetchHints_includeExtendedDataTableNames() {
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeExtendedDataTableNames(true).build(),
            new FetchHintsBuilder().setIncludeExtendedDataTableNames(true).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeExtendedDataTableNames(true).build(),
            new FetchHintsBuilder().setIncludeExtendedDataTableNames(false).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeExtendedDataTableNames(false).build(),
            new FetchHintsBuilder().setIncludeExtendedDataTableNames(false).build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIncludeExtendedDataTableNames(false).build(),
            new FetchHintsBuilder().setIncludeExtendedDataTableNames(true).build()
        );
    }

    @Test
    public void hasFetchHints_includeAllPropertyMetadata() {
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(true).build(),
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(true).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(true).build(),
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(false).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(false).build(),
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(false).build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(false).build(),
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(true).build()
        );
    }

    @Test
    public void hasFetchHints_includeAllProperties() {
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllProperties(true).build(),
            new FetchHintsBuilder().setIncludeAllProperties(true).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllProperties(true).build(),
            new FetchHintsBuilder().setIncludeAllProperties(false).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllProperties(false).build(),
            new FetchHintsBuilder().setIncludeAllProperties(false).build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIncludeAllProperties(false).build(),
            new FetchHintsBuilder().setIncludeAllProperties(true).build()
        );
    }

    @Test
    public void hasFetchHints_includeOutEdgeRefs() {
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeOutEdgeRefs(true).build(),
            new FetchHintsBuilder().setIncludeOutEdgeRefs(true).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllEdgeRefs(true).build(),
            new FetchHintsBuilder().setIncludeOutEdgeRefs(true).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeOutEdgeRefs(true).build(),
            new FetchHintsBuilder().setIncludeOutEdgeRefs(false).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeOutEdgeRefs(false).build(),
            new FetchHintsBuilder().setIncludeOutEdgeRefs(false).build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIncludeOutEdgeRefs(false).build(),
            new FetchHintsBuilder().setIncludeOutEdgeRefs(true).build()
        );
    }

    @Test
    public void hasFetchHints_includeInEdgeRefs() {
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeInEdgeRefs(true).build(),
            new FetchHintsBuilder().setIncludeInEdgeRefs(true).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllEdgeRefs(true).build(),
            new FetchHintsBuilder().setIncludeInEdgeRefs(true).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeInEdgeRefs(true).build(),
            new FetchHintsBuilder().setIncludeInEdgeRefs(false).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeInEdgeRefs(false).build(),
            new FetchHintsBuilder().setIncludeInEdgeRefs(false).build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIncludeInEdgeRefs(false).build(),
            new FetchHintsBuilder().setIncludeInEdgeRefs(true).build()
        );
    }

    @Test
    public void hasFetchHints_edgeRefs() {
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllEdgeRefs(true).build(),
            new FetchHintsBuilder().setIncludeAllEdgeRefs(true).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllEdgeRefs(true).build(),
            new FetchHintsBuilder().setIncludeAllEdgeRefs(false).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllEdgeRefs(false).build(),
            new FetchHintsBuilder().setIncludeAllEdgeRefs(false).build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIncludeAllEdgeRefs(false).build(),
            new FetchHintsBuilder().setIncludeAllEdgeRefs(true).build()
        );

        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllEdgeRefs(true).build(),
            new FetchHintsBuilder().setEdgeLabelsOfEdgeRefsToInclude("label1").build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIncludeAllEdgeRefs(false).build(),
            new FetchHintsBuilder().setEdgeLabelsOfEdgeRefsToInclude("label1").build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setEdgeLabelsOfEdgeRefsToInclude("label1", "label2").build(),
            new FetchHintsBuilder().setEdgeLabelsOfEdgeRefsToInclude("label1").build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setEdgeLabelsOfEdgeRefsToInclude("label1").build(),
            new FetchHintsBuilder().setEdgeLabelsOfEdgeRefsToInclude("label1", "label2").build()
        );
    }

    @Test
    public void hasFetchHints_propertyNames() {
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllProperties(true).build(),
            new FetchHintsBuilder().setIncludeAllProperties(true).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllProperties(true).build(),
            new FetchHintsBuilder().setIncludeAllProperties(false).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllProperties(false).build(),
            new FetchHintsBuilder().setIncludeAllProperties(false).build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIncludeAllProperties(false).build(),
            new FetchHintsBuilder().setIncludeAllProperties(true).build()
        );

        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllProperties(true).build(),
            new FetchHintsBuilder().setPropertyNamesToInclude("prop1").build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIncludeAllProperties(false).build(),
            new FetchHintsBuilder().setPropertyNamesToInclude("prop1").build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setPropertyNamesToInclude("prop1", "prop2").build(),
            new FetchHintsBuilder().setPropertyNamesToInclude("prop1").build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setPropertyNamesToInclude("prop1").build(),
            new FetchHintsBuilder().setPropertyNamesToInclude("prop1", "prop2").build()
        );
    }

    @Test
    public void hasFetchHints_metadataKeys() {
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(true).build(),
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(true).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(true).build(),
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(false).build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(false).build(),
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(false).build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(false).build(),
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(true).build()
        );

        assertHasFetchHints(
            new FetchHintsBuilder().setIncludeAllPropertyMetadata(true).build(),
            new FetchHintsBuilder().setMetadataKeysToInclude("prop1").build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setIncludeAllProperties(true).setIncludeAllPropertyMetadata(false).build(),
            new FetchHintsBuilder().setIncludeAllProperties(true).setMetadataKeysToInclude("prop1").build()
        );
        assertHasFetchHints(
            new FetchHintsBuilder().setMetadataKeysToInclude("prop1", "prop2").build(),
            new FetchHintsBuilder().setMetadataKeysToInclude("prop1").build()
        );
        assertDoesNotHaveFetchHints(
            new FetchHintsBuilder().setMetadataKeysToInclude("prop1").build(),
            new FetchHintsBuilder().setMetadataKeysToInclude("prop1", "prop2").build()
        );
    }

    private void assertDoesNotHaveFetchHints(FetchHints fetchHints, FetchHints fetchHintsTest) {
        if (fetchHints.hasFetchHints(fetchHintsTest)) {
            fail("Fetch hits\n" + fetchHints + "\nshould not have\n" + fetchHintsTest);
        }
    }

    private void assertHasFetchHints(FetchHints fetchHints, FetchHints fetchHintsTest) {
        if (!fetchHints.hasFetchHints(fetchHintsTest)) {
            fail("Fetch hits\n" + fetchHints + "\nshould have\n" + fetchHintsTest);
        }
    }
}
