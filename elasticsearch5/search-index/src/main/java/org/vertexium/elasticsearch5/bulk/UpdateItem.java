package org.vertexium.elasticsearch5.bulk;

import org.vertexium.ElementLocation;
import org.vertexium.VertexiumObjectId;
import org.vertexium.util.VertexiumLogger;
import org.vertexium.util.VertexiumLoggerFactory;

import java.util.Collection;
import java.util.Map;

import static org.vertexium.elasticsearch5.bulk.BulkUtils.*;

public class UpdateItem extends Item {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(UpdateItem.class);
    private final ElementLocation sourceElementLocation;
    private final Map<String, String> source;
    private final Map<String, Object> fieldsToSet;
    private final Collection<String> fieldsToRemove;
    private final Map<String, String> fieldsToRename;
    private final Collection<String> additionalVisibilities;
    private final Collection<String> additionalVisibilitiesToDelete;
    private final boolean existingElement;
    private final int size;

    public UpdateItem(
        String indexName,
        String type,
        String docId,
        VertexiumObjectId vertexiumObjectId,
        ElementLocation sourceElementLocation,
        Map<String, String> source,
        Map<String, Object> fieldsToSet,
        Collection<String> fieldsToRemove,
        Map<String, String> fieldsToRename,
        Collection<String> additionalVisibilities,
        Collection<String> additionalVisibilitiesToDelete,
        boolean existingElement
    ) {
        super(indexName, type, docId, vertexiumObjectId);
        this.sourceElementLocation = sourceElementLocation;
        this.source = source;
        this.fieldsToSet = fieldsToSet;
        this.fieldsToRemove = fieldsToRemove;
        this.fieldsToRename = fieldsToRename;
        this.additionalVisibilities = additionalVisibilities;
        this.additionalVisibilitiesToDelete = additionalVisibilitiesToDelete;
        this.existingElement = existingElement;

        this.size = getIndexName().length()
            + type.length()
            + docId.length()
            + calculateSizeOfId(vertexiumObjectId)
            + calculateSizeOfMap(source)
            + calculateSizeOfMap(fieldsToSet)
            + calculateSizeOfCollection(fieldsToRemove)
            + calculateSizeOfMap(fieldsToRename)
            + calculateSizeOfCollection(additionalVisibilities)
            + calculateSizeOfCollection(additionalVisibilitiesToDelete);
    }

    public Collection<String> getAdditionalVisibilities() {
        return additionalVisibilities;
    }

    public Collection<String> getAdditionalVisibilitiesToDelete() {
        return additionalVisibilitiesToDelete;
    }

    public Collection<String> getFieldsToRemove() {
        return fieldsToRemove;
    }

    public Map<String, String> getFieldsToRename() {
        return fieldsToRename;
    }

    public Map<String, Object> getFieldsToSet() {
        return fieldsToSet;
    }

    @Override
    public int getSize() {
        return size;
    }

    public Map<String, String> getSource() {
        return source;
    }

    public ElementLocation getSourceElementLocation() {
        return sourceElementLocation;
    }

    public boolean isExistingElement() {
        return existingElement;
    }
}
