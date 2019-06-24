package org.vertexium.elasticsearch5;

import org.vertexium.Element;
import org.vertexium.Metadata;
import org.vertexium.Property;
import org.vertexium.PropertyDefinition;
import org.vertexium.*;
import org.vertexium.elasticsearch5.models.TextIndexHint;
import org.vertexium.elasticsearch5.models.*;
import org.vertexium.mutation.AdditionalVisibilityAddMutation;
import org.vertexium.mutation.AdditionalVisibilityDeleteMutation;
import org.vertexium.mutation.PropertyDeleteMutation;
import org.vertexium.mutation.PropertySoftDeleteMutation;
import org.vertexium.mutation.*;
import org.vertexium.property.MutablePropertyImpl;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.type.GeoCircle;
import org.vertexium.type.GeoCollection;
import org.vertexium.type.GeoLine;
import org.vertexium.type.GeoPoint;
import org.vertexium.type.GeoPolygon;
import org.vertexium.type.GeoRect;
import org.vertexium.type.GeoShape;
import org.vertexium.type.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.vertexium.util.IterableUtils.toList;

public class ScriptService {
    private final PropertyNameService propertyNameService;
    private final StreamingPropertyValueService streamingPropertyValueService;

    public ScriptService(
        PropertyNameService propertyNameService,
        StreamingPropertyValueService streamingPropertyValueService
    ) {
        this.propertyNameService = propertyNameService;
        this.streamingPropertyValueService = streamingPropertyValueService;
    }

    public <T extends Element> Mutations mutationToScriptParameter(ElementMutation<T> mutation, long timestamp) {
        Mutations.Builder mutations = Mutations.newBuilder();

        mutations.addMutations(elementUpdateToMutation(mutation, timestamp));

        for (SetPropertyMetadata setPropertyMetadata : mutation.getSetPropertyMetadata()) {
            mutations.addMutations(setPropertyMetadataToMutation(setPropertyMetadata, timestamp));
        }
        for (AlterPropertyVisibility alterPropertyVisibility : mutation.getAlterPropertyVisibilities()) {
            mutations.addMutations(alterPropertyVisibilityToMutation(alterPropertyVisibility, timestamp));
        }
        if (mutation.getSoftDeleteData() != null) {
            mutations.addMutations(softDeleteDataToMutation(mutation.getSoftDeleteData(), timestamp));
        }
        for (Property property : mutation.getProperties()) {
            mutations.addMutations(propertyToMutation(property));
            for (Visibility hiddenVisibility : property.getHiddenVisibilities()) {
                throw new VertexiumElasticsearchException("not implemented");
            }
        }
        for (ElementMutationBase.MarkHiddenData markHidden : mutation.getMarkHiddenData()) {
            mutations.addMutations(markHiddenToMutation(markHidden, timestamp));
        }
        for (ElementMutationBase.MarkVisibleData markVisible : mutation.getMarkVisibleData()) {
            mutations.addMutations(markVisibleToMutation(markVisible, timestamp));
        }
        for (ElementMutationBase.MarkPropertyHiddenData markPropertyHidden : mutation.getMarkPropertyHiddenData()) {
            mutations.addMutations(markPropertyHiddenToMutation(markPropertyHidden, timestamp));
        }
        for (ElementMutationBase.MarkPropertyVisibleData markPropertyVisible : mutation.getMarkPropertyVisibleData()) {
            mutations.addMutations(markPropertyVisibleToMutation(markPropertyVisible, timestamp));
        }
        for (AdditionalVisibilityDeleteMutation additionalVisibilityDelete : mutation.getAdditionalVisibilityDeletes()) {
            mutations.addMutations(additionalVisibilityDeleteToMutation(additionalVisibilityDelete, timestamp));
        }
        for (AdditionalVisibilityAddMutation additionalVisibility : mutation.getAdditionalVisibilities()) {
            mutations.addMutations(additionalVisibilityAddToMutation(additionalVisibility, timestamp));
        }
        for (PropertySoftDeleteMutation propertySoftDelete : mutation.getPropertySoftDeletes()) {
            mutations.addMutations(propertySoftDeleteToMutation(propertySoftDelete));
        }
        for (PropertyDeleteMutation propertyDelete : mutation.getPropertyDeletes()) {
            mutations.addMutations(propertyDeleteToMutation(propertyDelete, timestamp));
        }
        if (mutation instanceof EdgeMutation && ((EdgeMutation) mutation).getNewEdgeLabel() != null) {
            mutations.addMutations(alterEdgeLabelMutationToMutation((EdgeMutation) mutation));
        }

        if (mutation instanceof ExistingElementMutation) {
            ExistingElementMutation existingElementMutation = (ExistingElementMutation) mutation;
            if (existingElementMutation.getNewElementVisibility() != null) {
                mutations.addMutations(alterElementVisibilityToMutation(
                    existingElementMutation.getNewElementVisibility(),
                    existingElementMutation.getNewElementVisibilityData(),
                    timestamp
                ));
            }
        }

        return mutations.build();
    }

    private <T extends Element> Mutation elementUpdateToMutation(ElementMutation<T> mutation, long timestamp) {
        Mutation.Builder builder = Mutation.newBuilder()
            .setTimestamp(timestamp);
        switch (mutation.getElementType()) {
            case VERTEX:
                builder.setUpdateVertexMutation(UpdateVertexMutation.newBuilder()
                    .setId(mutation.getId())
                    .setVisibility(mutation.getVisibility().getVisibilityString())
                    .build());
                break;
            case EDGE:
                EdgeMutation edgeMutation = (EdgeMutation) mutation;
                builder.setUpdateEdgeMutation(UpdateEdgeMutation.newBuilder()
                    .setId(mutation.getId())
                    .setVisibility(mutation.getVisibility().getVisibilityString())
                    .setOutVertexId(edgeMutation.getVertexId(Direction.OUT))
                    .setInVertexId(edgeMutation.getVertexId(Direction.IN))
                    .setLabel(
                        edgeMutation.getNewEdgeLabel() != null
                            ? edgeMutation.getNewEdgeLabel()
                            : edgeMutation.getEdgeLabel()
                    )
                    .build());
                break;
            default:
                throw new VertexiumException("unhandled element type: " + mutation.getElementType());
        }
        return builder // TODO mutation should probably timestamp
            .build();
    }

    private Mutation setPropertyMetadataToMutation(SetPropertyMetadata setPropertyMetadata, long timestamp) {
        SetPropertyMetadataMutation.Builder setPropertyMetadataMutation
            = SetPropertyMetadataMutation.newBuilder()
            .setPropertyKey(setPropertyMetadata.getPropertyKey())
            .setPropertyName(setPropertyMetadata.getPropertyName())
            .setMetadataKey(setPropertyMetadata.getMetadataName())
            .setMetadataVisibility(setPropertyMetadata.getMetadataVisibility().getVisibilityString())
            .setMetadataValue(objectToValue(setPropertyMetadata.getNewValue()));
        if (setPropertyMetadata.getPropertyVisibility() != null) {
            setPropertyMetadataMutation.setPropertyVisibility(
                setPropertyMetadata.getPropertyVisibility().getVisibilityString()
            );
        }
        return Mutation.newBuilder()
            .setTimestamp(timestamp)
            .setSetPropertyMetadataMutation(setPropertyMetadataMutation)
            .build();
    }

    private Mutation alterPropertyVisibilityToMutation(AlterPropertyVisibility alterPropertyVisibility, long timestamp) {
        AlterPropertyVisibilityMutation.Builder alterPropertyVisibilityMutation
            = AlterPropertyVisibilityMutation.newBuilder()
            .setKey(alterPropertyVisibility.getKey())
            .setName(alterPropertyVisibility.getName())
            .setNewVisibility(alterPropertyVisibility.getVisibility().getVisibilityString());
        if (alterPropertyVisibility.getExistingVisibility() != null) {
            alterPropertyVisibilityMutation.setOldVisibility(
                alterPropertyVisibility.getExistingVisibility().getVisibilityString()
            );
        }
        if (alterPropertyVisibility.getData() != null) {
            Value value = objectToValue(alterPropertyVisibility.getData());
            alterPropertyVisibilityMutation.setEventData(value);
        }
        return Mutation.newBuilder()
            .setTimestamp(timestamp)
            .setAlterPropertyVisibilityMutation(alterPropertyVisibilityMutation)
            .build();
    }

    private Mutation additionalVisibilityAddToMutation(
        AdditionalExtendedDataVisibilityAddMutation additionalExtendedDataVisibility,
        long timestamp
    ) {
        org.vertexium.elasticsearch5.models.AdditionalVisibilityAddMutation.Builder additionalVisibilityAddMutation
            = org.vertexium.elasticsearch5.models.AdditionalVisibilityAddMutation.newBuilder()
            .setVisibility(additionalExtendedDataVisibility.getAdditionalVisibility().getVisibilityString());
        if (additionalExtendedDataVisibility.getEventData() != null) {
            Value value = objectToValue(additionalExtendedDataVisibility.getEventData());
            additionalVisibilityAddMutation.setEventData(value);
        }
        return Mutation.newBuilder()
            .setTimestamp(timestamp)
            .setAdditionalVisibilityAddMutation(additionalVisibilityAddMutation)
            .build();
    }

    private Mutation additionalVisibilityAddToMutation(AdditionalVisibilityAddMutation additionalVisibility, long timestamp) {
        org.vertexium.elasticsearch5.models.AdditionalVisibilityAddMutation.Builder additionalVisibilityAddMutation
            = org.vertexium.elasticsearch5.models.AdditionalVisibilityAddMutation.newBuilder()
            .setVisibility(additionalVisibility.getAdditionalVisibility().getVisibilityString());
        if (additionalVisibility.getEventData() != null) {
            Value value = objectToValue(additionalVisibility.getEventData());
            additionalVisibilityAddMutation.setEventData(value);
        }
        return Mutation.newBuilder()
            .setTimestamp(timestamp)
            .setAdditionalVisibilityAddMutation(additionalVisibilityAddMutation)
            .build();
    }

    private Mutation additionalVisibilityDeleteToMutation(
        AdditionalExtendedDataVisibilityDeleteMutation additionalExtendedDataVisibilityDelete,
        long timestamp
    ) {
        org.vertexium.elasticsearch5.models.AdditionalVisibilityDeleteMutation.Builder additionalVisibilityDeleteMutation
            = org.vertexium.elasticsearch5.models.AdditionalVisibilityDeleteMutation.newBuilder()
            .setVisibility(additionalExtendedDataVisibilityDelete.getAdditionalVisibility().getVisibilityString());
        if (additionalExtendedDataVisibilityDelete.getEventData() != null) {
            Value value = objectToValue(additionalExtendedDataVisibilityDelete.getEventData());
            additionalVisibilityDeleteMutation.setEventData(value);
        }
        return Mutation.newBuilder()
            .setTimestamp(timestamp)
            .setAdditionalVisibilityDeleteMutation(additionalVisibilityDeleteMutation)
            .build();
    }

    private Mutation additionalVisibilityDeleteToMutation(AdditionalVisibilityDeleteMutation additionalVisibilityDelete, long timestamp) {
        org.vertexium.elasticsearch5.models.AdditionalVisibilityDeleteMutation.Builder additionalVisibilityDeleteMutation
            = org.vertexium.elasticsearch5.models.AdditionalVisibilityDeleteMutation.newBuilder()
            .setVisibility(additionalVisibilityDelete.getAdditionalVisibility().getVisibilityString());
        if (additionalVisibilityDelete.getEventData() != null) {
            Value value = objectToValue(additionalVisibilityDelete.getEventData());
            additionalVisibilityDeleteMutation.setEventData(value);
        }
        return Mutation.newBuilder()
            .setTimestamp(timestamp)
            .setAdditionalVisibilityDeleteMutation(additionalVisibilityDeleteMutation)
            .build();
    }

    private Mutation alterElementVisibilityToMutation(Visibility newElementVisibility, Object eventData, long timestamp) {
        AlterElementVisibilityMutation.Builder alterElementVisibilityMutation = AlterElementVisibilityMutation.newBuilder()
            .setVisibility(newElementVisibility.getVisibilityString());
        if (eventData != null) {
            Value value = objectToValue(eventData);
            alterElementVisibilityMutation.setEventData(value);
        }
        return Mutation.newBuilder()
            .setTimestamp(timestamp)
            .setAlterElementVisibilityMutation(alterElementVisibilityMutation)
            .build();
    }

    private Mutation markPropertyHiddenToMutation(ElementMutationBase.MarkPropertyHiddenData markPropertyHidden, long timestamp) {
        MarkPropertyHiddenMutation.Builder markPropertyHiddenMutation = MarkPropertyHiddenMutation.newBuilder()
            .setVisibility(markPropertyHidden.getVisibility().getVisibilityString())
            .setPropertyVisibility(markPropertyHidden.getPropertyVisibility().getVisibilityString())
            .setKey(markPropertyHidden.getKey())
            .setName(markPropertyHidden.getName());
        if (markPropertyHidden.getEventData() != null) {
            Value value = objectToValue(markPropertyHidden.getEventData());
            markPropertyHiddenMutation.setEventData(value);
        }
        return Mutation.newBuilder()
            .setTimestamp(markPropertyHidden.getTimestamp() == null ? timestamp : markPropertyHidden.getTimestamp())
            .setMarkPropertyHiddenMutation(markPropertyHiddenMutation)
            .build();
    }

    private Mutation markPropertyVisibleToMutation(ElementMutationBase.MarkPropertyVisibleData markPropertyVisible, long timestamp) {
        MarkPropertyVisibleMutation.Builder markPropertyVisibleMutation = MarkPropertyVisibleMutation.newBuilder()
            .setVisibility(markPropertyVisible.getVisibility().getVisibilityString())
            .setPropertyVisibility(markPropertyVisible.getPropertyVisibility().getVisibilityString())
            .setKey(markPropertyVisible.getKey())
            .setName(markPropertyVisible.getName());
        if (markPropertyVisible.getEventData() != null) {
            Value value = objectToValue(markPropertyVisible.getEventData());
            markPropertyVisibleMutation.setEventData(value);
        }
        return Mutation.newBuilder()
            .setTimestamp(markPropertyVisible.getTimestamp() == null ? timestamp : markPropertyVisible.getTimestamp())
            .setMarkPropertyVisibleMutation(markPropertyVisibleMutation)
            .build();
    }

    private Mutation markVisibleToMutation(ElementMutationBase.MarkVisibleData markVisible, long timestamp) {
        MarkVisibleMutation.Builder markVisibleMutation = MarkVisibleMutation.newBuilder()
            .setVisibility(markVisible.getVisibility().getVisibilityString());
        if (markVisible.getEventData() != null) {
            Value value = objectToValue(markVisible.getEventData());
            markVisibleMutation.setEventData(value);
        }
        return Mutation.newBuilder()
            .setTimestamp(markVisible.getTimestamp() == null ? timestamp : markVisible.getTimestamp())
            .setMarkVisibleMutation(markVisibleMutation)
            .build();
    }

    private Mutation markHiddenToMutation(ElementMutationBase.MarkHiddenData markHidden, long timestamp) {
        MarkHiddenMutation.Builder markHiddenMutation = MarkHiddenMutation.newBuilder()
            .setVisibility(markHidden.getVisibility().getVisibilityString());
        if (markHidden.getEventData() != null) {
            Value value = objectToValue(markHidden.getEventData());
            markHiddenMutation.setEventData(value);
        }
        return Mutation.newBuilder()
            .setTimestamp(markHidden.getTimestamp() == null ? timestamp : markHidden.getTimestamp())
            .setMarkHiddenMutation(markHiddenMutation)
            .build();
    }

    public Mutations extendedDataToScriptParameters(
        List<ExtendedDataMutation> extendedData,
        List<ExtendedDataDeleteMutation> extendedDataDeletes,
        List<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        List<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes,
        Visibility newElementVisibility,
        long timestamp
    ) {
        Mutations.Builder mutations = Mutations.newBuilder();
        if (extendedData != null) {
            for (ExtendedDataMutation data : extendedData) {
                mutations.addMutations(extendedDataMutation(data));
            }
        }
        if (extendedDataDeletes != null) {
            for (ExtendedDataDeleteMutation extendedDataDelete : extendedDataDeletes) {
                mutations.addMutations(extendedDataDeleteToMutation(extendedDataDelete, timestamp));
            }
        }
        if (additionalExtendedDataVisibilities != null) {
            for (AdditionalExtendedDataVisibilityAddMutation additionalExtendedDataVisibility : additionalExtendedDataVisibilities) {
                mutations.addMutations(additionalVisibilityAddToMutation(additionalExtendedDataVisibility, timestamp));
            }
        }
        if (additionalExtendedDataVisibilityDeletes != null) {
            for (AdditionalExtendedDataVisibilityDeleteMutation additionalExtendedDataVisibilityDelete : additionalExtendedDataVisibilityDeletes) {
                mutations.addMutations(additionalVisibilityDeleteToMutation(additionalExtendedDataVisibilityDelete, timestamp));
            }
        }
        if (newElementVisibility != null) {
            mutations.addMutations(alterElementVisibilityToMutation(newElementVisibility, null, timestamp));
        }
        return mutations.build();
    }

    private Mutation propertySoftDeleteToMutation(PropertySoftDeleteMutation propertySoftDelete) {
        org.vertexium.elasticsearch5.models.PropertySoftDeleteMutation.Builder propertySoftDeleteMutation
            = org.vertexium.elasticsearch5.models.PropertySoftDeleteMutation.newBuilder()
            .setKey(propertySoftDelete.getKey())
            .setName(propertySoftDelete.getName())
            .setVisibility(propertySoftDelete.getVisibility().getVisibilityString());
        if (propertySoftDelete.getData() != null) {
            Value value = objectToValue(propertySoftDelete.getData());
            propertySoftDeleteMutation.setEventData(value);
        }
        return Mutation.newBuilder()
            .setTimestamp(propertySoftDelete.getTimestamp())
            .setPropertySoftDeleteMutation(propertySoftDeleteMutation)
            .build();
    }

    private Mutation extendedDataDeleteToMutation(ExtendedDataDeleteMutation extendedDataDelete, long timestamp) {
        org.vertexium.elasticsearch5.models.PropertyDeleteMutation.Builder builder
            = org.vertexium.elasticsearch5.models.PropertyDeleteMutation.newBuilder()
            .setName(extendedDataDelete.getColumnName())
            .setVisibility(extendedDataDelete.getVisibility().getVisibilityString());
        if (extendedDataDelete.getKey() != null) {
            builder.setKey(extendedDataDelete.getKey());
        }
        return Mutation.newBuilder()
            .setTimestamp(timestamp)
            .setPropertyDeleteMutation(builder)
            .build();
    }

    private Mutation propertyDeleteToMutation(PropertyDeleteMutation propertyDelete, long timestamp) {
        org.vertexium.elasticsearch5.models.PropertyDeleteMutation propertyDeleteMutation
            = org.vertexium.elasticsearch5.models.PropertyDeleteMutation.newBuilder()
            .setKey(propertyDelete.getKey())
            .setName(propertyDelete.getName())
            .setVisibility(propertyDelete.getVisibility().getVisibilityString())
            .build();
        return Mutation.newBuilder()
            .setTimestamp(timestamp)
            .setPropertyDeleteMutation(propertyDeleteMutation)
            .build();
    }

    private Mutation extendedDataMutation(ExtendedDataMutation data) {
        SetPropertyMutation.Builder setPropertyMutation = SetPropertyMutation.newBuilder()
            .setName(data.getColumnName())
            .setVisibility(data.getVisibility().getVisibilityString())
            .setValue(objectToValue(data.getValue()));
        if (data.getKey() != null) {
            setPropertyMutation.setKey(data.getKey());
        }
        return Mutation.newBuilder()
            .setTimestamp(data.getTimestamp())
            .setSetPropertyMutation(setPropertyMutation)
            .build();
    }

    private Mutation propertyToMutation(Property property) {
        SetPropertyMutation.Builder setPropertyMutation = SetPropertyMutation.newBuilder()
            .setKey(property.getKey())
            .setName(property.getName())
            .setVisibility(property.getVisibility().getVisibilityString())
            .setValue(objectToValue(property.getValue()));

        for (Metadata.Entry entry : property.getMetadata().entrySet()) {
            setPropertyMutation.addMetadata(metadataEntryToScriptParameter(entry));
        }
        return Mutation.newBuilder()
            .setTimestamp(property.getTimestamp())
            .setSetPropertyMutation(setPropertyMutation)
            .build();
    }

    private MetadataEntry metadataEntryToScriptParameter(Metadata.Entry entry) {
        return MetadataEntry.newBuilder()
            .setKey(entry.getKey())
            .setVisibility(entry.getVisibility().getVisibilityString())
            .setValue(objectToValue(entry.getValue()))
            .build();
    }

    private Mutation softDeleteDataToMutation(ElementMutationBase.SoftDeleteData softDeleteData, long timestamp) {
        SoftDeleteMutation.Builder softDeleteMutation = SoftDeleteMutation.newBuilder();
        if (softDeleteData.getEventData() != null) {
            Value value = objectToValue(softDeleteData.getEventData());
            softDeleteMutation.setEventData(value);
        }
        return Mutation.newBuilder()
            .setTimestamp(softDeleteData.getTimestamp() == null ? timestamp : softDeleteData.getTimestamp())
            .setSoftDeleteMutation(softDeleteMutation)
            .build();
    }

    private Mutation alterEdgeLabelMutationToMutation(EdgeMutation mutation) {
        return Mutation.newBuilder()
            .setTimestamp(mutation.getAlterEdgeLabelTimestamp())
            .setAlterEdgeLabelMutation(
                AlterEdgeLabelMutation.newBuilder()
                    .setNewEdgeLabel(mutation.getNewEdgeLabel())
            )
            .build();
    }

    public Value objectToValue(Object obj) {
        Value.Builder builder = Value.newBuilder();
        if (obj instanceof StreamingPropertyValue) {
            builder.setStreamingPropertyValueRef(streamingPropertyValueToProtobuf((StreamingPropertyValue) obj));
        } else if (obj instanceof String) {
            builder.setStringValue((String) obj);
        } else if (obj instanceof GeoShape) {
            builder.setGeoShape(geoShapeToProtobuf((GeoShape) obj));
        } else if (obj instanceof Integer) {
            builder.setInt32Value((Integer) obj);
        } else if (obj instanceof Short) {
            builder.setInt16Value((Short) obj);
        } else if (obj instanceof Byte) {
            builder.setByteValue((Byte) obj);
        } else if (obj instanceof Long) {
            builder.setInt64Value((Long) obj);
        } else if (obj instanceof BigInteger) {
            builder.setBigIntegerValue(((BigInteger) obj).toString());
        } else if (obj instanceof BigDecimal) {
            builder.setBigDecimalValue(((BigDecimal) obj).toString());
        } else if (obj instanceof Double) {
            builder.setDoubleValue((Double) obj);
        } else if (obj instanceof Float) {
            builder.setFloatValue((Float) obj);
        } else if (obj instanceof DateOnly) {
            builder.setDateOnly(((DateOnly) obj).getDate().getTime());
        } else if (obj instanceof Date) {
            builder.setDate(((Date) obj).getTime());
        } else if (obj instanceof Boolean) {
            builder.setBoolValue(((Boolean) obj));
        } else if (obj instanceof IpV4Address) {
            IpV4Address addr = (IpV4Address) obj;
            builder.setIpv4Address(addr.toString());
        } else if (obj instanceof Iterable) {
            List list = toList((Iterable) obj);
            ListValue.Builder listValueBuilder = ListValue.newBuilder();
            for (Object o : list) {
                listValueBuilder.addValues(objectToValue(o));
            }
            builder.setListValue(listValueBuilder);
        } else if (obj instanceof PropertyDefinition) {
            PropertyDefinition propertyDefinition = (PropertyDefinition) obj;
            org.vertexium.elasticsearch5.models.PropertyDefinition.Builder propertyDefinitionBuilder
                = org.vertexium.elasticsearch5.models.PropertyDefinition.newBuilder()
                .setPropertyName(propertyDefinition.getPropertyName())
                .setDataType(classToSimpleTypeName(propertyDefinition.getDataType()))
                .setSortable(propertyDefinition.isSortable());
            if (propertyDefinition.getTextIndexHints() != null) {
                propertyDefinitionBuilder.addAllTextIndexHints(
                    propertyDefinition.getTextIndexHints().stream()
                        .map(tih -> {
                            switch (tih) {
                                case FULL_TEXT:
                                    return TextIndexHint.FULL_TEXT;
                                case EXACT_MATCH:
                                    return TextIndexHint.EXACT_MATCH;
                                default:
                                    throw new VertexiumException("Unhandled text index hint: " + tih);
                            }
                        })
                        .collect(Collectors.toList())
                );
            }
            if (propertyDefinition.getBoost() != null) {
                propertyDefinitionBuilder.setBoost(propertyDefinition.getBoost());
            }
            builder.setPropertyDefinitionValue(propertyDefinitionBuilder);
        } else {
            throw new VertexiumElasticsearchException("not implemented: " + (obj == null ? null : obj.getClass().getName()));
        }
        return builder.build();
    }

    private String classToSimpleTypeName(Class clazz) {
        if (clazz.equals(String.class)) {
            return "string";
        }
        if (clazz.equals(Double.class)) {
            return "double";
        }
        if (clazz.equals(Integer.class)) {
            return "integer";
        }
        if (clazz.equals(Boolean.class)) {
            return "boolean";
        }
        if (clazz.equals(Date.class)) {
            return "date";
        }
        if (clazz.equals(DateOnly.class)) {
            return "dateOnly";
        }
        if (clazz.equals(Short.class)) {
            return "short";
        }
        if (clazz.equals(Byte.class)) {
            return "byte";
        }
        return clazz.getName();
    }

    private Class simpleTypeNameToClass(String simpleTypeName) {
        switch (simpleTypeName) {
            case "string":
                return String.class;
            case "double":
                return Double.class;
            case "integer":
                return Integer.class;
            case "boolean":
                return Boolean.class;
            case "date":
                return Date.class;
            case "dateOnly":
                return DateOnly.class;
            case "short":
                return Short.class;
            case "byte":
                return Byte.class;
        }
        try {
            return Class.forName(simpleTypeName);
        } catch (ClassNotFoundException e) {
            throw new VertexiumException("Could not find type: " + simpleTypeName, e);
        }
    }

    private org.vertexium.elasticsearch5.models.StreamingPropertyValueRef streamingPropertyValueToProtobuf(
        StreamingPropertyValue spv
    ) {
        return streamingPropertyValueService.save(spv);
    }

    private org.vertexium.elasticsearch5.models.GeoShape geoShapeToProtobuf(GeoShape obj) {
        if (obj instanceof GeoPoint) {
            return geoPointToProtobufGeoShape((GeoPoint) obj);
        } else if (obj instanceof GeoCircle) {
            return geoCircleToProtobuf((GeoCircle) obj);
        } else if (obj instanceof GeoPolygon) {
            return geoPolygonToProtobuf((GeoPolygon) obj);
        } else if (obj instanceof GeoLine) {
            return geoLineToProtobuf((GeoLine) obj);
        } else if (obj instanceof GeoCollection) {
            return geoCollectionToProtobuf((GeoCollection) obj);
        } else if (obj instanceof GeoRect) {
            return geoRectToProtobuf((GeoRect) obj);
        } else {
            throw new VertexiumElasticsearchException("unhandled type: " + obj.getClass().getName());
        }
    }

    private org.vertexium.elasticsearch5.models.GeoShape geoCollectionToProtobuf(GeoCollection geoCollection) {
        org.vertexium.elasticsearch5.models.GeoShape.Builder geoShapeBuilder = org.vertexium.elasticsearch5.models.GeoShape.newBuilder();

        if (geoCollection.getDescription() != null) {
            geoShapeBuilder.setDescription(geoCollection.getDescription());
        }

        org.vertexium.elasticsearch5.models.GeoCollection.Builder geoPointBuilder = org.vertexium.elasticsearch5.models.GeoCollection.newBuilder()
            .addAllShapes(
                geoCollection.getGeoShapes().stream()
                    .map(this::geoShapeToProtobuf)
                    .collect(Collectors.toList())
            );

        return geoShapeBuilder
            .setGeoCollection(geoPointBuilder)
            .build();
    }

    private org.vertexium.elasticsearch5.models.GeoShape geoRectToProtobuf(GeoRect geoRect) {
        org.vertexium.elasticsearch5.models.GeoShape.Builder geoShapeBuilder = org.vertexium.elasticsearch5.models.GeoShape.newBuilder();

        if (geoRect.getDescription() != null) {
            geoShapeBuilder.setDescription(geoRect.getDescription());
        }

        org.vertexium.elasticsearch5.models.GeoRect.Builder geoPointBuilder = org.vertexium.elasticsearch5.models.GeoRect.newBuilder()
            .setNorthWest(geoPointToProtobuf(geoRect.getNorthWest()))
            .setSouthEast(geoPointToProtobuf(geoRect.getSouthEast()));

        return geoShapeBuilder
            .setGeoRect(geoPointBuilder)
            .build();
    }

    private org.vertexium.elasticsearch5.models.GeoShape geoLineToProtobuf(GeoLine geoLine) {
        org.vertexium.elasticsearch5.models.GeoShape.Builder geoShapeBuilder = org.vertexium.elasticsearch5.models.GeoShape.newBuilder();

        if (geoLine.getDescription() != null) {
            geoShapeBuilder.setDescription(geoLine.getDescription());
        }

        org.vertexium.elasticsearch5.models.GeoLine.Builder geoPointBuilder = org.vertexium.elasticsearch5.models.GeoLine.newBuilder()
            .addAllPoints(
                geoLine.getGeoPoints().stream()
                    .map(ScriptService::geoPointToProtobuf)
                    .collect(Collectors.toList())
            );

        return geoShapeBuilder
            .setGeoLine(geoPointBuilder)
            .build();
    }

    private org.vertexium.elasticsearch5.models.GeoShape geoPolygonToProtobuf(GeoPolygon geoPolygon) {
        org.vertexium.elasticsearch5.models.GeoShape.Builder geoShapeBuilder = org.vertexium.elasticsearch5.models.GeoShape.newBuilder();

        if (geoPolygon.getDescription() != null) {
            geoShapeBuilder.setDescription(geoPolygon.getDescription());
        }

        org.vertexium.elasticsearch5.models.GeoPolygon.Builder geoPointBuilder = org.vertexium.elasticsearch5.models.GeoPolygon.newBuilder();
        geoPointBuilder.addAllOuterBoundary(
            geoPolygon.getOuterBoundary().stream()
                .map(ScriptService::geoPointToProtobuf)
                .collect(Collectors.toList())
        );
        geoPointBuilder.addAllHoles(
            geoPolygon.getHoles().stream()
                .map(ScriptService::geoPolygonHoleToProtobuf)
                .collect(Collectors.toList())
        );
        return geoShapeBuilder
            .setGeoPolygon(geoPointBuilder)
            .build();
    }

    private static GeoPolygonHole geoPolygonHoleToProtobuf(List<GeoPoint> geoPoints) {
        return GeoPolygonHole.newBuilder()
            .addAllPoints(
                geoPoints.stream()
                    .map(ScriptService::geoPointToProtobuf)
                    .collect(Collectors.toList())
            )
            .build();
    }

    private org.vertexium.elasticsearch5.models.GeoShape geoCircleToProtobuf(GeoCircle geoCircle) {
        org.vertexium.elasticsearch5.models.GeoShape.Builder geoShapeBuilder = org.vertexium.elasticsearch5.models.GeoShape.newBuilder();

        if (geoCircle.getDescription() != null) {
            geoShapeBuilder.setDescription(geoCircle.getDescription());
        }

        org.vertexium.elasticsearch5.models.GeoCircle.Builder geoPointBuilder = org.vertexium.elasticsearch5.models.GeoCircle.newBuilder()
            .setLatitude(geoCircle.getLatitude())
            .setLongitude(geoCircle.getLongitude())
            .setRadius(geoCircle.getRadius());

        return geoShapeBuilder
            .setGeoCircle(geoPointBuilder)
            .build();
    }

    private static org.vertexium.elasticsearch5.models.GeoShape geoPointToProtobufGeoShape(GeoPoint geoPoint) {
        org.vertexium.elasticsearch5.models.GeoShape.Builder geoShapeBuilder = org.vertexium.elasticsearch5.models.GeoShape.newBuilder();

        if (geoPoint.getDescription() != null) {
            geoShapeBuilder.setDescription(geoPoint.getDescription());
        }

        return geoShapeBuilder
            .setGeoPoint(geoPointToProtobuf(geoPoint))
            .build();
    }

    private static org.vertexium.elasticsearch5.models.GeoPoint geoPointToProtobuf(GeoPoint geoPoint) {
        org.vertexium.elasticsearch5.models.GeoPoint.Builder geoPointBuilder = org.vertexium.elasticsearch5.models.GeoPoint.newBuilder()
            .setLatitude(geoPoint.getLatitude())
            .setLongitude(geoPoint.getLongitude());
        if (geoPoint.getAccuracy() != null) {
            geoPointBuilder.setAccuracy(geoPoint.getAccuracy());
        }
        if (geoPoint.getAltitude() != null) {
            geoPointBuilder.setAltitude(geoPoint.getAltitude());
        }
        return geoPointBuilder.build();
    }

    public Object valueToJavaObject(Value value) {
        switch (value.getValueCase()) {
            case STREAMING_PROPERTY_VALUE_REF:
                return streamingPropertyValueService.fromProtobuf(value.getStreamingPropertyValueRef());

            case STRING_VALUE:
                return value.getStringValue();

            case GEO_SHAPE:
                return protobufGeoShapeToVertexium(value.getGeoShape());

            case INT32_VALUE:
                return value.getInt32Value();

            case DATE:
                return new Date(value.getDate());

            case DATE_ONLY:
                return new DateOnly(new Date(value.getDate()));

            case IPV4_ADDRESS:
                return new IpV4Address(value.getIpv4Address());

            case BIG_DECIMAL_VALUE:
                return new BigDecimal(value.getBigDecimalValue());

            case BIG_INTEGER_VALUE:
                return new BigInteger(value.getBigIntegerValue());

            case BOOL_VALUE:
                return value.getBoolValue();

            case BYTE_VALUE:
                return (byte) value.getByteValue();

            case DOUBLE_VALUE:
                return value.getDoubleValue();

            case FLOAT_VALUE:
                return (float) value.getFloatValue();

            case INT16_VALUE:
                return (short) value.getInt16Value();

            case INT64_VALUE:
                return value.getInt64Value();

            case LIST_VALUE:
                return protobufListToVertexium(value.getListValue());

            case PROPERTY_DEFINITION_VALUE:
                return protobufPropertyDefinitionToVertexium(value.getPropertyDefinitionValue());

            default:
                throw new VertexiumElasticsearchException("not implemented: " + value);
        }
    }

    private PropertyDefinition protobufPropertyDefinitionToVertexium(
        org.vertexium.elasticsearch5.models.PropertyDefinition propertyDefinition
    ) {
        return new PropertyDefinition(
            propertyDefinition.getPropertyName(),
            simpleTypeNameToClass(propertyDefinition.getDataType()),
            propertyDefinition.getTextIndexHintsList().stream()
                .map(tih -> {
                    switch (tih) {
                        case FULL_TEXT:
                            return org.vertexium.TextIndexHint.FULL_TEXT;
                        case EXACT_MATCH:
                            return org.vertexium.TextIndexHint.EXACT_MATCH;
                        default:
                            throw new VertexiumException("unhandled text index hint: " + tih);
                    }
                })
                .collect(Collectors.toSet()),
            propertyDefinition.hasBoost() ? propertyDefinition.getBoost() : null,
            propertyDefinition.hasSortable() ? propertyDefinition.getSortable() : false
        );
    }

    private List<Object> protobufListToVertexium(ListValue listValue) {
        return listValue.getValuesList().stream()
            .map(this::valueToJavaObject)
            .collect(Collectors.toList());
    }

    private static GeoShape protobufGeoShapeToVertexium(org.vertexium.elasticsearch5.models.GeoShape geoShape) {
        switch (geoShape.getGeoShapeCase()) {
            case GEO_POINT:
                return protobufGeoPointToVertexium(geoShape, geoShape.getGeoPoint());
            case GEO_CIRCLE:
                return protobufGeoCircleToVertexium(geoShape, geoShape.getGeoCircle());
            case GEO_POLYGON:
                return protobufGeoPolygonToVertexium(geoShape, geoShape.getGeoPolygon());
            case GEO_LINE:
                return protobufGeoLineToVertexium(geoShape, geoShape.getGeoLine());
            case GEO_COLLECTION:
                return protobufGeoCollectionToVertexium(geoShape, geoShape.getGeoCollection());
            case GEO_RECT:
                return protobufGeoRectToVertexium(geoShape, geoShape.getGeoRect());
            default:
                throw new VertexiumElasticsearchException("unhandled case: " + geoShape.getGeoShapeCase());
        }
    }

    private static GeoCollection protobufGeoCollectionToVertexium(
        org.vertexium.elasticsearch5.models.GeoShape geoShape,
        org.vertexium.elasticsearch5.models.GeoCollection geoCollection
    ) {
        return new GeoCollection(
            geoCollection.getShapesList().stream()
                .map(ScriptService::protobufGeoShapeToVertexium)
                .collect(Collectors.toList()),
            geoShape.hasDescription() ? geoShape.getDescription() : null
        );
    }

    private static GeoRect protobufGeoRectToVertexium(
        org.vertexium.elasticsearch5.models.GeoShape geoShape,
        org.vertexium.elasticsearch5.models.GeoRect geoRect
    ) {
        return new GeoRect(
            protobufGeoPointToVertexium(null, geoRect.getNorthWest()),
            protobufGeoPointToVertexium(null, geoRect.getSouthEast()),
            geoShape.hasDescription() ? geoShape.getDescription() : null
        );
    }

    private static GeoLine protobufGeoLineToVertexium(
        org.vertexium.elasticsearch5.models.GeoShape geoShape,
        org.vertexium.elasticsearch5.models.GeoLine geoLine
    ) {
        return new GeoLine(
            geoLine.getPointsList().stream()
                .map(gp -> protobufGeoPointToVertexium(null, gp))
                .collect(Collectors.toList()),
            geoShape.hasDescription() ? geoShape.getDescription() : null
        );
    }

    private static GeoPolygon protobufGeoPolygonToVertexium(
        org.vertexium.elasticsearch5.models.GeoShape geoShape,
        org.vertexium.elasticsearch5.models.GeoPolygon geoPolygon
    ) {
        return new GeoPolygon(
            geoPolygon.getOuterBoundaryList().stream()
                .map(gp -> protobufGeoPointToVertexium(null, gp))
                .collect(Collectors.toList()),
            geoPolygon.getHolesList().stream()
                .map(ScriptService::protobufGeoPolygonHoleToVertexium)
                .collect(Collectors.toList()),
            geoShape.hasDescription() ? geoShape.getDescription() : null
        );
    }

    private static List<GeoPoint> protobufGeoPolygonHoleToVertexium(GeoPolygonHole hole) {
        return hole.getPointsList().stream()
            .map(gp -> protobufGeoPointToVertexium(null, gp))
            .collect(Collectors.toList());
    }

    private static GeoCircle protobufGeoCircleToVertexium(
        org.vertexium.elasticsearch5.models.GeoShape geoShape,
        org.vertexium.elasticsearch5.models.GeoCircle geoCircle
    ) {
        return new GeoCircle(
            geoCircle.getLatitude(),
            geoCircle.getLongitude(),
            geoCircle.getRadius(),
            geoShape.hasDescription() ? geoShape.getDescription() : null
        );
    }

    private static GeoPoint protobufGeoPointToVertexium(
        org.vertexium.elasticsearch5.models.GeoShape geoShape,
        org.vertexium.elasticsearch5.models.GeoPoint geoPoint
    ) {
        return new GeoPoint(
            geoPoint.getLatitude(),
            geoPoint.getLongitude(),
            geoPoint.hasAltitude() ? geoPoint.getAltitude() : null,
            geoPoint.hasAccuracy() ? geoPoint.getAccuracy() : null,
            geoShape != null && geoShape.hasDescription() ? geoShape.getDescription() : null
        );
    }

    public <T extends Element> Mutation deleteMutationToMutation(ElementMutation<T> mutation, long timestamp) {
        DeleteMutation.Builder deleteMutation = DeleteMutation.newBuilder();
        if (mutation.isDeleteElement()) {
            deleteMutation.setSoftDelete(false);
        } else if (mutation.getSoftDeleteData() != null) {
            Long softDeleteTimestamp = mutation.getSoftDeleteData().getTimestamp();
            timestamp = softDeleteTimestamp == null ? timestamp : softDeleteTimestamp;
            deleteMutation.setSoftDelete(true);
            if (mutation.getSoftDeleteData().getEventData() != null) {
                deleteMutation.setEventData(objectToValue(mutation.getSoftDeleteData().getEventData()));
            }
        } else {
            throw new VertexiumException("unhandled mutation: " + mutation);
        }
        return Mutation.newBuilder()
            .setTimestamp(timestamp)
            .setDeleteMutation(deleteMutation)
            .build();
    }

    @SuppressWarnings("unchecked")
    public org.vertexium.Property protobufPropertyToVertexium(org.vertexium.elasticsearch5.models.Property prop, FetchHints fetchHints) {
        Set<Visibility> hiddenVisibilities = prop.getHiddenVisibilitiesList() == null
            ? Collections.emptySet()
            : prop.getHiddenVisibilitiesList().stream()
            .map(hv -> new Visibility(hv.getVisibility()))
            .collect(Collectors.toSet());
        Object value = valueToJavaObject(prop.getValue());
        if (value instanceof org.vertexium.property.StreamingPropertyValueRef) {
            org.vertexium.property.StreamingPropertyValueRef<Elasticsearch5Graph> ref = (org.vertexium.property.StreamingPropertyValueRef) value;
            value = streamingPropertyValueService.read(ref, prop.getTimestamp());
        }
        return new MutablePropertyImpl(
            prop.getKey(),
            prop.getName(),
            value,
            protobufMetadataToVertexium(prop.getMetadataList(), fetchHints),
            prop.getTimestamp(),
            hiddenVisibilities,
            new Visibility(prop.getVisibility()),
            fetchHints
        );
    }

    public org.vertexium.Metadata protobufMetadataToVertexium(List<MetadataEntry> metadata, FetchHints fetchHints) {
        org.vertexium.Metadata results = new MapMetadata(fetchHints);
        if (metadata == null) {
            return results;
        }
        for (MetadataEntry entry : metadata) {
            results.add(
                entry.getKey(),
                valueToJavaObject(entry.getValue()),
                new Visibility(entry.getVisibility())
            );
        }
        return results;
    }
}
