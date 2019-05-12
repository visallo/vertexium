package org.vertexium.elasticsearch5.utils;

import com.google.protobuf.InvalidProtocolBufferException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.SearchHitField;
import org.vertexium.elasticsearch5.VertexiumElasticsearchException;
import org.vertexium.elasticsearch5.models.*;

import java.util.Base64;
import java.util.function.Function;
import java.util.function.Supplier;

public class ProtobufUtils {
    public static <T> T fromField(Object fieldValue, Function<byte[], T> parseFromBytes, Supplier<T> defaultSupplier) {
        if (fieldValue == null) {
            return defaultSupplier.get();
        }
        byte[] bytes;
        if (fieldValue instanceof byte[]) {
            bytes = (byte[]) fieldValue;
        } else if (fieldValue instanceof String) {
            bytes = Base64.getDecoder().decode((String) fieldValue);
        } else if (fieldValue instanceof BytesArray) {
            bytes = ((BytesArray) fieldValue).array();
        } else if (fieldValue instanceof SearchHitField) {
            SearchHitField searchHitField = (SearchHitField) fieldValue;
            if (searchHitField.getValues().size() == 1) {
                return fromField(searchHitField.getValues().get(0), parseFromBytes, defaultSupplier);
            } else if (searchHitField.getValues().size() == 0) {
                return fromField(null, parseFromBytes, defaultSupplier);
            } else {
                throw new VertexiumElasticsearchException(String.format(
                    "invalid field value: %s (expected 1 value found %d)",
                    fieldValue.getClass().getName(),
                    searchHitField.getValues().size()
                ));
            }
        } else {
            throw new VertexiumElasticsearchException("invalid field value: " + fieldValue.getClass().getName());
        }
        return parseFromBytes.apply(bytes);
    }

    public static Properties propertiesFromField(Object value) {
        return fromField(
            value,
            bytes -> {
                try {
                    return Properties.parseFrom(bytes);
                } catch (InvalidProtocolBufferException e) {
                    throw new VertexiumElasticsearchException("Could not parse properties", e);
                }
            },
            () -> Properties.newBuilder().build()
        );
    }

    public static HiddenData hiddenDataFromField(Object value) {
        return fromField(
            value,
            bytes -> {
                try {
                    return HiddenData.parseFrom(bytes);
                } catch (InvalidProtocolBufferException e) {
                    throw new VertexiumElasticsearchException("Could not parse hidden data", e);
                }
            },
            () -> HiddenData.newBuilder().build()
        );
    }

    public static Mutations mutationsFromField(Object value) {
        return fromField(
            value,
            bytes -> {
                try {
                    return Mutations.parseFrom(bytes);
                } catch (InvalidProtocolBufferException e) {
                    throw new VertexiumElasticsearchException("Could not parse mutations", e);
                }
            },
            () -> Mutations.newBuilder().build()
        );
    }

    public static Mutation mutationFromField(Object value) {
        return fromField(
            value,
            bytes -> {
                try {
                    return Mutation.parseFrom(bytes);
                } catch (InvalidProtocolBufferException e) {
                    throw new VertexiumElasticsearchException("Could not parse mutation", e);
                }
            },
            () -> null
        );
    }

    public static Value valueFromField(Object value) {
        return fromField(
            value,
            bytes -> {
                try {
                    return Value.parseFrom(bytes);
                } catch (InvalidProtocolBufferException e) {
                    throw new VertexiumElasticsearchException("Could not parse value", e);
                }
            },
            () -> null
        );
    }

    public static AdditionalVisibilities additionalVisibilitiesFromField(Object value) {
        return fromField(
            value,
            bytes -> {
                try {
                    return AdditionalVisibilities.parseFrom(bytes);
                } catch (InvalidProtocolBufferException e) {
                    throw new VertexiumElasticsearchException("Could not parse additional visibilities", e);
                }
            },
            () -> AdditionalVisibilities.newBuilder().build()
        );
    }
}
