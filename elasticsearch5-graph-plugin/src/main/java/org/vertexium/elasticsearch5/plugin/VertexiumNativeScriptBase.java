package org.vertexium.elasticsearch5.plugin;

import org.elasticsearch.common.geo.builders.CircleBuilder;
import org.elasticsearch.script.ExecutableScript;
import org.vertexium.elasticsearch5.FieldNames;
import org.vertexium.elasticsearch5.ParameterNames;
import org.vertexium.elasticsearch5.VertexiumElasticsearchException;
import org.vertexium.elasticsearch5.models.Properties;
import org.vertexium.elasticsearch5.models.*;
import org.vertexium.elasticsearch5.utils.PropertyNameUtils;
import org.vertexium.elasticsearch5.utils.ProtobufUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.common.geo.builders.ShapeBuilder.*;

public abstract class VertexiumNativeScriptBase implements ExecutableScript {
    private static final String FIELDNAME_DOT_REPLACEMENT = "-_-";
    private final Map<String, Object> params;
    private Map<String, Object> ctx;
    private Map<String, Object> source;

    public VertexiumNativeScriptBase(Map<String, Object> params) {
        this.params = params;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setNextVar(String name, Object value) {
        if ("ctx".equals(name)) {
            ctx = (Map<String, Object>) value;
            source = (Map<String, Object>) ctx.get("_source");
        }
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public Map<String, Object> getCtx() {
        return ctx;
    }

    public Map<String, Object> getSource() {
        return source;
    }

    @Override
    public Object run() {
        Properties sourcePropertiesObj = ProtobufUtils.propertiesFromField(getSource().get(FieldNames.PROPERTIES_DATA));
        List<Property> sourceProperties;
        if (sourcePropertiesObj.getPropertiesList() == null) {
            sourceProperties = new ArrayList<>();
        } else {
            sourceProperties = new ArrayList<>(sourcePropertiesObj.getPropertiesList());
        }

        HiddenData hiddenVisibilitiesObj = ProtobufUtils.hiddenDataFromField(getSource().get(FieldNames.HIDDEN_VISIBILITY_DATA));
        List<HiddenDataItem> hiddenVisibilities;
        if (hiddenVisibilitiesObj.getHiddenDataList() == null) {
            hiddenVisibilities = new ArrayList<>();
        } else {
            hiddenVisibilities = new ArrayList<>(hiddenVisibilitiesObj.getHiddenDataList());
        }

        AdditionalVisibilities additionalVisibilitiesObj = ProtobufUtils.additionalVisibilitiesFromField(getSource().get(FieldNames.ADDITIONAL_VISIBILITY_DATA));
        List<AdditionalVisibility> additionalVisibilities;
        if (additionalVisibilitiesObj.getAdditionalVisibilitiesList() == null) {
            additionalVisibilities = new ArrayList<>();
        } else {
            additionalVisibilities = new ArrayList<>(additionalVisibilitiesObj.getAdditionalVisibilitiesList());
        }

        Mutations mutations = ProtobufUtils.mutationsFromField(getParams().get(ParameterNames.UPDATE));

        for (Mutation mutation : mutations.getMutationsList()) {
            switch (mutation.getMutationCase()) {
                case UPDATE_VERTEX_MUTATION:
                case UPDATE_EDGE_MUTATION:
                case DELETE_MUTATION:
                    break;

                case SET_PROPERTY_MUTATION:
                    applySetPropertyMutation(sourceProperties, mutation.getTimestamp(), mutation.getSetPropertyMutation());
                    break;

                case PROPERTY_DELETE_MUTATION:
                    applyPropertyDeleteMutation(sourceProperties, mutation.getTimestamp(), mutation.getPropertyDeleteMutation());
                    break;

                case ALTER_EDGE_LABEL_MUTATION:
                    applyAlterEdgeLabelMutation(mutation.getAlterEdgeLabelMutation());
                    break;

                case PROPERTY_SOFT_DELETE_MUTATION:
                    applyPropertySoftDeleteMutation(sourceProperties, mutation.getTimestamp(), mutation.getPropertySoftDeleteMutation());
                    break;

                case SOFT_DELETE_MUTATION:
                    applySoftDeleteMutation(mutation.getTimestamp(), mutation.getSoftDeleteMutation());
                    break;

                case MARK_HIDDEN_MUTATION:
                    applyMarkHiddenMutation(hiddenVisibilities, mutation.getTimestamp(), mutation.getMarkHiddenMutation());
                    break;

                case MARK_VISIBLE_MUTATION:
                    applyMarkVisibleMutation(hiddenVisibilities, mutation.getTimestamp(), mutation.getMarkVisibleMutation());
                    break;

                case MARK_PROPERTY_HIDDEN_MUTATION:
                    applyMarkPropertyHiddenMutation(sourceProperties, mutation.getTimestamp(), mutation.getMarkPropertyHiddenMutation());
                    break;

                case MARK_PROPERTY_VISIBLE_MUTATION:
                    applyMarkPropertyVisibleMutation(sourceProperties, mutation.getTimestamp(), mutation.getMarkPropertyVisibleMutation());
                    break;

                case ALTER_ELEMENT_VISIBILITY_MUTATION:
                    applyAlterElementVisibilityMutation(mutation.getTimestamp(), mutation.getAlterElementVisibilityMutation());
                    break;

                case ADDITIONAL_VISIBILITY_ADD_MUTATION:
                    applyAdditionalVisibilityAddMutation(
                        additionalVisibilities,
                        mutation.getTimestamp(),
                        mutation.getAdditionalVisibilityAddMutation()
                    );
                    break;

                case ADDITIONAL_VISIBILITY_DELETE_MUTATION:
                    applyAdditionalVisibilityDeleteMutation(
                        additionalVisibilities,
                        mutation.getTimestamp(),
                        mutation.getAdditionalVisibilityDeleteMutation()
                    );
                    break;

                case ALTER_PROPERTY_VISIBILITY_MUTATION:
                    applyAlterPropertyVisibilityMutation(
                        sourceProperties,
                        mutation.getTimestamp(),
                        mutation.getAlterPropertyVisibilityMutation()
                    );
                    break;

                case SET_PROPERTY_METADATA_MUTATION:
                    applySetPropertyMetadataMutation(
                        sourceProperties,
                        mutation.getTimestamp(),
                        mutation.getSetPropertyMetadataMutation()
                    );
                    break;

                default:
                    throw new VertexiumElasticsearchException("not implemented: " + mutation);
            }
        }

        updateSourceWithHiddenVisibilities(hiddenVisibilities);
        updateSourceWithAdditionalVisibilities(additionalVisibilities);
        updateSourceWithProperties(sourceProperties);
        return null;
    }

    private void applyAlterEdgeLabelMutation(AlterEdgeLabelMutation alterEdgeLabelMutation) {
        getSource().put(FieldNames.EDGE_LABEL, alterEdgeLabelMutation.getNewEdgeLabel());
    }

    private void applyAdditionalVisibilityAddMutation(
        List<AdditionalVisibility> additionalVisibilities,
        long timestamp,
        AdditionalVisibilityAddMutation mutation
    ) {
        additionalVisibilities.removeIf(av -> av.getVisibility().equals(mutation.getVisibility()));
        AdditionalVisibility.Builder additionalVisibility = AdditionalVisibility.newBuilder()
            .setVisibility(mutation.getVisibility());
        if (mutation.hasEventData()) {
            additionalVisibility.setEventData(mutation.getEventData());
        }
        additionalVisibilities.add(additionalVisibility.build());
    }

    private void applyAdditionalVisibilityDeleteMutation(
        List<AdditionalVisibility> additionalVisibilities,
        long timestamp,
        AdditionalVisibilityDeleteMutation mutation
    ) {
        additionalVisibilities.removeIf(av -> av.getVisibility().equals(mutation.getVisibility()));
    }

    private void applyAlterElementVisibilityMutation(long timestamp, AlterElementVisibilityMutation alterElementVisibilityMutation) {
        String oldElementTypeSourceKey = findElementTypeWithHashSourceKey();
        String elementType = (String) getSource().get(oldElementTypeSourceKey);
        getSource().remove(oldElementTypeSourceKey);
        String newElementTypePropertyNameWithVisibilityHash
            = PropertyNameUtils.getPropertyNameWithVisibility(FieldNames.ELEMENT_TYPE, alterElementVisibilityMutation.getVisibility());
        getSource().put(newElementTypePropertyNameWithVisibilityHash, elementType);
        getSource().put(FieldNames.ELEMENT_VISIBILITY, alterElementVisibilityMutation.getVisibility());
    }

    private String findElementTypeWithHashSourceKey() {
        String prefix = FieldNames.ELEMENT_TYPE + "_";
        for (String sourceKey : getSource().keySet()) {
            if (sourceKey.startsWith(prefix)) {
                return sourceKey;
            }
        }
        return null;
    }

    private void applySoftDeleteMutation(long timestamp, SoftDeleteMutation softDeleteMutation) {
        SoftDeleteData.Builder softDeleteData = SoftDeleteData.newBuilder();
        if (softDeleteMutation.hasEventData()) {
            softDeleteData.setEventData(softDeleteMutation.getEventData());
        }
        getSource().put(
            FieldNames.SOFT_DELETE_DATA,
            softDeleteData.build().toByteArray()
        );
    }

    private void updateSourceWithAdditionalVisibilities(List<AdditionalVisibility> additionalVisibilities) {
        getSource().put(
            FieldNames.ADDITIONAL_VISIBILITY,
            additionalVisibilities.stream()
                .map(AdditionalVisibility::getVisibility)
                .collect(Collectors.toList())
        );
        getSource().put(
            FieldNames.ADDITIONAL_VISIBILITY_DATA,
            AdditionalVisibilities.newBuilder()
                .addAllAdditionalVisibilities(additionalVisibilities)
                .build().toByteArray()
        );
    }

    private void updateSourceWithHiddenVisibilities(List<HiddenDataItem> hiddenVisibilities) {
        for (HiddenDataItem hiddenVisibility : hiddenVisibilities) {
            String name = PropertyNameUtils.getPropertyNameWithVisibility(FieldNames.HIDDEN_ELEMENT, hiddenVisibility.getVisibility());
            getSource().put(name, true);
        }
        getSource().put(
            FieldNames.HIDDEN_VISIBILITY_DATA,
            HiddenData.newBuilder()
                .addAllHiddenData(hiddenVisibilities)
                .build().toByteArray()
        );
    }

    private void applyMarkHiddenMutation(
        List<HiddenDataItem> hiddenVisibilities,
        long timestamp,
        MarkHiddenMutation markHiddenMutation
    ) {
        hiddenVisibilities.removeIf(hv -> hv.getVisibility().equals(markHiddenMutation.getVisibility()));
        HiddenDataItem.Builder builder = HiddenDataItem.newBuilder()
            .setVisibility(markHiddenMutation.getVisibility());
        if (markHiddenMutation.hasEventData()) {
            builder.setEventData(markHiddenMutation.getEventData());
        }
        hiddenVisibilities.add(
            builder
                .build()
        );
    }

    private void applyMarkVisibleMutation(
        List<HiddenDataItem> hiddenVisibilities,
        long timestamp,
        MarkVisibleMutation markVisibleMutation
    ) {
        hiddenVisibilities.removeIf(hv -> hv.getVisibility().equals(markVisibleMutation.getVisibility()));
        String name = PropertyNameUtils.getPropertyNameWithVisibility(FieldNames.HIDDEN_ELEMENT, markVisibleMutation.getVisibility());
        getSource().remove(name);
    }

    private void applyPropertyDeleteMutation(
        List<Property> properties,
        long timestamp,
        PropertyDeleteMutation mutation
    ) {
        removeProperty(properties, mutation.getKey(), mutation.getName(), mutation.getVisibility());
    }

    private void applyMarkPropertyHiddenMutation(
        List<Property> sourceProperties,
        long timestamp,
        MarkPropertyHiddenMutation markPropertyHiddenMutation
    ) {
        removePropertyFromSource(markPropertyHiddenMutation.getName(), markPropertyHiddenMutation.getPropertyVisibility());
        Property property = getProperty(
            sourceProperties,
            markPropertyHiddenMutation.getKey(),
            markPropertyHiddenMutation.getName(),
            markPropertyHiddenMutation.getPropertyVisibility()
        );
        if (property == null) {
            throw new VertexiumElasticsearchException("Could not find property: " + property);
        }
        sourceProperties.remove(property);

        Property.Builder newProperty = Property.newBuilder(property);
        PropertyHiddenVisibility.Builder propertyHiddenVisibility = PropertyHiddenVisibility.newBuilder()
            .setVisibility(markPropertyHiddenMutation.getVisibility());
        if (markPropertyHiddenMutation.hasEventData()) {
            propertyHiddenVisibility.setEventData(markPropertyHiddenMutation.getEventData());
        }
        newProperty.addHiddenVisibilities(propertyHiddenVisibility);
        sourceProperties.add(newProperty.build());
    }

    private void applyMarkPropertyVisibleMutation(
        List<Property> sourceProperties,
        long timestamp,
        MarkPropertyVisibleMutation markPropertyVisibleMutation
    ) {
        removePropertyFromSource(markPropertyVisibleMutation.getName(), markPropertyVisibleMutation.getPropertyVisibility());
        Property property = getProperty(
            sourceProperties,
            markPropertyVisibleMutation.getKey(),
            markPropertyVisibleMutation.getName(),
            markPropertyVisibleMutation.getPropertyVisibility()
        );
        if (property == null) {
            throw new VertexiumElasticsearchException("Could not find property: " + property);
        }
        sourceProperties.remove(property);

        Property.Builder newProperty = Property.newBuilder(property);
        Integer propertyHiddenVisibilityIndex = getHiddenVisiblityIndex(newProperty, markPropertyVisibleMutation.getVisibility());
        if (propertyHiddenVisibilityIndex != null) {
            newProperty.removeHiddenVisibilities(propertyHiddenVisibilityIndex);
        }
        sourceProperties.add(newProperty.build());
    }

    private void applyAlterPropertyVisibilityMutation(
        List<Property> sourceProperties,
        long timestamp,
        AlterPropertyVisibilityMutation alterPropertyVisibility
    ) {
        Property property = getProperty(
            sourceProperties,
            alterPropertyVisibility.getKey(),
            alterPropertyVisibility.getName(),
            alterPropertyVisibility.getNewVisibility()
        );
        if (property != null) {
            if (property.getVisibility().equals(alterPropertyVisibility.getNewVisibility())) {
                return;
            }

            removePropertyFromSource(property.getName(), property.getVisibility());
            sourceProperties.remove(property);
        }

        property = getProperty(
            sourceProperties,
            alterPropertyVisibility.getKey(),
            alterPropertyVisibility.getName(),
            alterPropertyVisibility.hasOldVisibility() ? alterPropertyVisibility.getOldVisibility() : null
        );
        if (property == null) {
            throw new VertexiumElasticsearchException("Could not find property: " + property);
        }
        removePropertyFromSource(property.getName(), property.getVisibility());
        sourceProperties.remove(property);

        Property.Builder newProperty = Property.newBuilder(property)
            .setVisibility(alterPropertyVisibility.getNewVisibility());
        sourceProperties.add(newProperty.build());
    }

    private void applySetPropertyMetadataMutation(
        List<Property> sourceProperties,
        long timestamp,
        SetPropertyMetadataMutation mutation
    ) {
        Property property = getProperty(
            sourceProperties,
            mutation.getPropertyKey(),
            mutation.getPropertyName(),
            mutation.hasPropertyVisibility() ? mutation.getPropertyVisibility() : null
        );
        if (property == null) {
            throw new VertexiumElasticsearchException("Could not find property: " + property);
        }
        removePropertyFromSource(property.getName(), property.getVisibility());
        sourceProperties.remove(property);

        List<MetadataEntry> metadata = new ArrayList<>(property.getMetadataList());
        metadata.removeIf(m ->
            m.getKey().equals(mutation.getMetadataKey())
                && m.getVisibility().equals(mutation.getMetadataVisibility())
        );
        metadata.add(MetadataEntry.newBuilder()
            .setKey(mutation.getMetadataKey())
            .setVisibility(mutation.getMetadataVisibility())
            .setValue(mutation.getMetadataValue())
            .build());

        Property.Builder newProperty = Property.newBuilder(property)
            .addAllMetadata(metadata);
        sourceProperties.add(newProperty.build());
    }

    private Integer getHiddenVisiblityIndex(Property.Builder property, String visibility) {
        List<PropertyHiddenVisibility> hiddenVisibilities = property.getHiddenVisibilitiesList();
        for (int i = 0; i < hiddenVisibilities.size(); i++) {
            if (hiddenVisibilities.get(i).getVisibility().equals(visibility)) {
                return i;
            }
        }
        return null;
    }

    private void applyPropertySoftDeleteMutation(
        List<Property> sourceProperties,
        long timestamp,
        PropertySoftDeleteMutation propertySoftDeleteMutation
    ) {
        removePropertyFromSource(propertySoftDeleteMutation.getName(), propertySoftDeleteMutation.getVisibility());
        Property property = getProperty(
            sourceProperties,
            propertySoftDeleteMutation.getKey(),
            propertySoftDeleteMutation.getName(),
            propertySoftDeleteMutation.getVisibility()
        );
        if (property == null) {
            throw new VertexiumElasticsearchException("Could not find property: " + property);
        }
        sourceProperties.remove(property);

        Property.Builder newProperty = Property.newBuilder(property);
        newProperty.setSoftDelete(true);
        if (propertySoftDeleteMutation.hasEventData()) {
            newProperty.setSoftDeleteEventData(propertySoftDeleteMutation.getEventData());
        } else {
            newProperty.clearSoftDeleteEventData();
        }
        sourceProperties.add(newProperty.build());
    }

    private void applySetPropertyMutation(List<Property> properties, long timestamp, SetPropertyMutation mutation) {
        removeProperty(properties, mutation.getKey(), mutation.getName(), mutation.getVisibility());
        Property.Builder builder = Property.newBuilder()
            .setKey(mutation.getKey())
            .setName(mutation.getName())
            .setVisibility(mutation.getVisibility())
            .setTimestamp(timestamp)
            .setValue(mutation.getValue());
        for (MetadataEntry metadataEntry : mutation.getMetadataList()) {
            builder.addMetadata(metadataEntry);
        }
        properties.add(builder.build());
    }

    private void removeProperty(List<Property> properties, String key, String name, String visibility) {
        removePropertyFromSource(name, visibility);
        properties.removeIf(property ->
            property.getKey().equals(key)
                && property.getName().equals(name)
                && property.getVisibility().equals(visibility)
        );
    }

    private Property getProperty(
        List<Property> sourceProperties,
        String key,
        String name,
        String visibility
    ) {
        return sourceProperties.stream()
            .filter(property -> property.getKey().equals(key)
                && property.getName().equals(name)
                && (visibility == null || property.getVisibility().equals(visibility)))
            .findFirst()
            .orElse(null);
    }

    private void removePropertyFromSource(String name, String visibility) {
        String propertyNameWithVisibility = PropertyNameUtils.getPropertyNameWithVisibility(name, visibility);
        getSource().remove(propertyNameWithVisibility + FieldNames.GEO_POINT_PROPERTY_NAME_SUFFIX);
        getSource().remove(propertyNameWithVisibility + FieldNames.GEO_PROPERTY_NAME_SUFFIX);
        getSource().remove(propertyNameWithVisibility);
    }

    protected void updateSourceWithProperties(List<Property> sourceProperties) {
        Map<String, List<Object>> additionalValues = new HashMap<>();
        Map<String, List<Property>> propertiesByNameWithVisibility = getPropertiesByNameWithVisibility(sourceProperties);
        for (Map.Entry<String, List<Property>> entry : propertiesByNameWithVisibility.entrySet()) {
            String nameWithVisibility = entry.getKey();
            List<Object> values = new ArrayList<>();
            for (Property property : entry.getValue()) {
                if (property.getSoftDelete()) {
                    continue;
                }
                Value value = property.getValue();
                switch (value.getValueCase()) {
                    case STREAMING_PROPERTY_VALUE_REF:
                        StreamingPropertyValueRef streamingPropertyValueRef = value.getStreamingPropertyValueRef();
                        if (streamingPropertyValueRef.hasStringValue()) {
                            values.add(streamingPropertyValueRef.getStringValue());
                        }
                        break;

                    case GEO_SHAPE:
                        GeoShape geoShape = value.getGeoShape();
                        if (geoShape.hasDescription()) {
                            values.add(geoShape.getDescription());
                        }

                        Map<String, Object> propertyValueMap;
                        switch (geoShape.getGeoShapeCase()) {
                            case GEO_POINT:
                                GeoPoint geoPoint = geoShape.getGeoPoint();
                                Map<String, Double> coordinates = new HashMap<>();
                                coordinates.put("lat", geoPoint.getLatitude());
                                coordinates.put("lon", geoPoint.getLongitude());
                                List<Object> l = additionalValues.computeIfAbsent(
                                    nameWithVisibility + FieldNames.GEO_POINT_PROPERTY_NAME_SUFFIX,
                                    (key) -> new ArrayList<>()
                                );
                                l.add(coordinates);
                                propertyValueMap = convertGeoPoint(geoPoint);
                                break;

                            case GEO_CIRCLE:
                                propertyValueMap = convertGeoCircle(geoShape.getGeoCircle());
                                break;

                            case GEO_POLYGON:
                                propertyValueMap = convertGeoPolygon(geoShape.getGeoPolygon());
                                break;

                            case GEO_LINE:
                                propertyValueMap = convertGeoLine(geoShape.getGeoLine());
                                break;

                            case GEO_RECT:
                                propertyValueMap = convertGeoRect(geoShape.getGeoRect());
                                break;

                            case GEO_COLLECTION:
                                propertyValueMap = convertGeoCollection(geoShape.getGeoCollection());
                                break;

                            default:
                                throw new VertexiumElasticsearchException("not implemented:" + value);
                        }

                        additionalValues.computeIfAbsent(
                            nameWithVisibility + FieldNames.GEO_PROPERTY_NAME_SUFFIX,
                            (key) -> new ArrayList<>()
                        ).add(propertyValueMap);
                        break;

                    case STRING_VALUE:
                        values.add(value.getStringValue());
                        break;

                    case INT32_VALUE:
                        values.add(value.getInt32Value());
                        break;

                    case INT64_VALUE:
                        values.add(value.getInt64Value());
                        break;

                    case DOUBLE_VALUE:
                        values.add(value.getDoubleValue());
                        break;

                    case BOOL_VALUE:
                        values.add(value.getBoolValue());
                        break;

                    case DATE_ONLY:
                        values.add(value.getDateOnly());
                        break;

                    case DATE:
                        values.add(value.getDate());
                        break;

                    case IPV4_ADDRESS:
                        values.add(value.getIpv4Address());
                        break;

                    case BIG_DECIMAL_VALUE:
                        values.add(new BigDecimal(value.getBigDecimalValue()).doubleValue());
                        break;

                    case BIG_INTEGER_VALUE:
                        values.add(new BigInteger(value.getBigIntegerValue()).longValue());
                        break;

                    case BYTE_VALUE:
                        values.add(value.getByteValue());
                        break;

                    case FLOAT_VALUE:
                        values.add(value.getFloatValue());
                        break;

                    case INT16_VALUE:
                        values.add(value.getInt16Value());
                        break;

                    default:
                        throw new VertexiumElasticsearchException("not implemented:" + value);
                }
            }
            source.put(nameWithVisibility, values);
        }
        for (Map.Entry<String, List<Object>> additionalValuesEntry : additionalValues.entrySet()) {
            source.put(additionalValuesEntry.getKey(), additionalValuesEntry.getValue());
        }
        source.put(
            FieldNames.PROPERTIES_DATA,
            Properties.newBuilder()
                .addAllProperties(sourceProperties)
                .build()
                .toByteArray()
        );
    }

    private Map<String, List<Property>> getPropertiesByNameWithVisibility(List<Property> sourceProperties) {
        Map<String, List<Property>> results = new HashMap<>();
        for (Property property : sourceProperties) {
            String nameWithVisibility = PropertyNameUtils.getPropertyNameWithVisibility(property);
            results.computeIfAbsent(nameWithVisibility, s -> new ArrayList<>())
                .add(property);
        }
        return results;
    }

    private Map<String, Object> convertGeoCollection(GeoCollection geoCollection) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(FIELD_TYPE, "geometrycollection");

        List<Map<String, Object>> geometries = new ArrayList<>();
        geoCollection.getShapesList().forEach(gs -> {
            switch (gs.getGeoShapeCase()) {
                case GEO_POINT:
                    geometries.add(convertGeoPoint(gs.getGeoPoint()));
                    break;
                case GEO_CIRCLE:
                    geometries.add(convertGeoCircle(gs.getGeoCircle()));
                    break;
                case GEO_LINE:
                    geometries.add(convertGeoLine(gs.getGeoLine()));
                    break;
                case GEO_RECT:
                    geometries.add(convertGeoRect(gs.getGeoRect()));
                    break;
                case GEO_POLYGON:
                    geometries.add(convertGeoPolygon(gs.getGeoPolygon()));
                    break;
                default:
                    throw new VertexiumElasticsearchPluginException("Unsupported GeoShape value in GeoCollection of type: " + gs.getClass().getName());
            }
        });
        propertyValueMap.put(FIELD_GEOMETRIES, geometries);
        return propertyValueMap;
    }

    private Map<String, Object> convertGeoPolygon(GeoPolygon geoPolygon) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(FIELD_TYPE, "polygon");
        List<List<List<Double>>> coordinates = new ArrayList<>();
        coordinates.add(geoPolygon.getOuterBoundaryList().stream()
            .map(geoPoint -> Arrays.asList(geoPoint.getLongitude(), geoPoint.getLatitude()))
            .collect(Collectors.toList()));
        geoPolygon.getHolesList().forEach(holeBoundary ->
            coordinates.add(holeBoundary.getPointsList().stream()
                .map(geoPoint -> Arrays.asList(geoPoint.getLongitude(), geoPoint.getLatitude()))
                .collect(Collectors.toList())));
        propertyValueMap.put(FIELD_COORDINATES, coordinates);
        return propertyValueMap;
    }

    private Map<String, Object> convertGeoRect(GeoRect geoRect) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(FIELD_TYPE, "envelope");
        List<List<Double>> coordinates = new ArrayList<>();
        coordinates.add(Arrays.asList(geoRect.getNorthWest().getLongitude(), geoRect.getNorthWest().getLatitude()));
        coordinates.add(Arrays.asList(geoRect.getSouthEast().getLongitude(), geoRect.getSouthEast().getLatitude()));
        propertyValueMap.put(FIELD_COORDINATES, coordinates);
        return propertyValueMap;
    }

    private Map<String, Object> convertGeoLine(GeoLine geoLine) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(FIELD_TYPE, "linestring");
        List<List<Double>> coordinates = new ArrayList<>();
        geoLine.getPointsList()
            .forEach(geoPoint -> coordinates.add(Arrays.asList(geoPoint.getLongitude(), geoPoint.getLatitude())));
        propertyValueMap.put(FIELD_COORDINATES, coordinates);
        return propertyValueMap;
    }

    private Map<String, Object> convertGeoCircle(GeoCircle geoCircle) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(FIELD_TYPE, "circle");
        List<Double> coordinates = new ArrayList<>();
        coordinates.add(geoCircle.getLongitude());
        coordinates.add(geoCircle.getLatitude());
        propertyValueMap.put(FIELD_COORDINATES, coordinates);
        propertyValueMap.put(CircleBuilder.FIELD_RADIUS, geoCircle.getRadius() + "km");
        return propertyValueMap;
    }

    private Map<String, Object> convertGeoPoint(GeoPoint geoPoint) {
        Map<String, Object> propertyValueMap = new HashMap<>();
        propertyValueMap.put(FIELD_TYPE, "point");
        propertyValueMap.put(FIELD_COORDINATES, Arrays.asList(geoPoint.getLongitude(), geoPoint.getLatitude()));
        return propertyValueMap;
    }

    @SuppressWarnings("unchecked")
    protected Set<String> fieldToSet(Object o) {
        if (o == null) {
            return new HashSet<>();
        }
        if (o instanceof Set) {
            return (Set<String>) o;
        }
        if (o instanceof Collection) {
            HashSet<String> result = new HashSet<>();
            result.addAll((Collection<String>) o);
            return result;
        }
        throw new VertexiumElasticsearchPluginException("Could not convert field to set: " + o);
    }

    public String replaceFieldnameDots(String fieldName) {
        return fieldName.replace(".", FIELDNAME_DOT_REPLACEMENT);
    }
}
