package org.vertexium.elasticsearch5.plugin;

import org.vertexium.elasticsearch5.FieldNames;
import org.vertexium.elasticsearch5.models.Property;
import org.vertexium.elasticsearch5.utils.PropertyNameUtils;

import java.util.*;

public class VertexiumSaveExtendedDataMutationNativeScript extends VertexiumNativeScriptBase {
    public VertexiumSaveExtendedDataMutationNativeScript(Map<String, Object> params) {
        super(params);
    }

    @Override
    protected void updateSourceWithProperties(List<Property> sourceProperties) {
        super.updateSourceWithProperties(sourceProperties);

        Set<String> propertyVisibilities = new HashSet<>();
        for (Property sourceProperty : sourceProperties) {
            String hash = PropertyNameUtils.getVisibilityHash(sourceProperty.getVisibility());
            if (hash != null) {
                propertyVisibilities.add(hash);
            }
        }
        List<String> columnVisibilitiesList = new ArrayList<>(propertyVisibilities);
        getSource().put(FieldNames.EXTENDED_DATA_TABLE_PROPERTY_VISIBILITIES, columnVisibilitiesList);
    }
}
