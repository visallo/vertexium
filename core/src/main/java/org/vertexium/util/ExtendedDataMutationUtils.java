package org.vertexium.util;

import org.vertexium.mutation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExtendedDataMutationUtils {
    public static Map<String, Map<String, Mutations>> getByTableThenRowId(
        Iterable<ExtendedDataMutation> extendedData,
        Iterable<ExtendedDataDeleteMutation> extendedDataDeletes,
        Iterable<ElementMutationBase.DeleteExtendedDataRowData> extendedDataRowDeletes,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes
    ) {
        Map<String, Map<String, Mutations>> results = new HashMap<>();

        if (extendedData != null) {
            for (ExtendedDataMutation m : extendedData) {
                Map<String, Mutations> byTable = results.computeIfAbsent(m.getTableName(), s -> new HashMap<>());
                Mutations byRow = byTable.computeIfAbsent(m.getRow(), s -> new Mutations());
                byRow.addExtendedDataMutation(m);
            }
        }

        if (extendedDataDeletes != null) {
            for (ExtendedDataDeleteMutation m : extendedDataDeletes) {
                Map<String, Mutations> byTable = results.computeIfAbsent(m.getTableName(), s -> new HashMap<>());
                Mutations byRow = byTable.computeIfAbsent(m.getRow(), s -> new Mutations());
                byRow.addExtendedDataDeleteMutation(m);
            }
        }

        if (extendedDataRowDeletes != null) {
            for (ElementMutationBase.DeleteExtendedDataRowData m : extendedDataRowDeletes) {
                Map<String, Mutations> byTable = results.computeIfAbsent(m.getTableName(), s -> new HashMap<>());
                Mutations byRow = byTable.computeIfAbsent(m.getRow(), s -> new Mutations());
                byRow.addExtendedDataRowDeleteMutation(m);
            }
        }

        if (additionalExtendedDataVisibilities != null) {
            for (AdditionalExtendedDataVisibilityAddMutation m : additionalExtendedDataVisibilities) {
                Map<String, Mutations> byTable = results.computeIfAbsent(m.getTableName(), s -> new HashMap<>());
                Mutations byRow = byTable.computeIfAbsent(m.getRow(), s -> new Mutations());
                byRow.addAdditionalExtendedDataVisibilityAddMutation(m);
            }
        }

        if (additionalExtendedDataVisibilityDeletes != null) {
            for (AdditionalExtendedDataVisibilityDeleteMutation m : additionalExtendedDataVisibilityDeletes) {
                Map<String, Mutations> byTable = results.computeIfAbsent(m.getTableName(), s -> new HashMap<>());
                Mutations byRow = byTable.computeIfAbsent(m.getRow(), s -> new Mutations());
                byRow.addAdditionalExtendedDataVisibilityDeleteMutation(m);
            }
        }

        return results;
    }

    public static class Mutations {
        private final List<ExtendedDataMutation> extendedData = new ArrayList<>();
        private final List<ExtendedDataDeleteMutation> extendedDataDeletes = new ArrayList<>();
        private final List<ElementMutationBase.DeleteExtendedDataRowData> extendedDataRowDeletes = new ArrayList<>();
        private final List<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities = new ArrayList<>();
        private final List<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes = new ArrayList<>();

        public Iterable<ExtendedDataMutation> getExtendedData() {
            return extendedData;
        }

        public Iterable<ExtendedDataDeleteMutation> getExtendedDataDeletes() {
            return extendedDataDeletes;
        }

        public List<ElementMutationBase.DeleteExtendedDataRowData> getExtendedDataRowDeletes() {
            return extendedDataRowDeletes;
        }

        public Iterable<AdditionalExtendedDataVisibilityAddMutation> getAdditionalExtendedDataVisibilities() {
            return additionalExtendedDataVisibilities;
        }

        public Iterable<AdditionalExtendedDataVisibilityDeleteMutation> getAdditionalExtendedDataVisibilityDeletes() {
            return additionalExtendedDataVisibilityDeletes;
        }

        public void addExtendedDataMutation(ExtendedDataMutation extendedDataMutation) {
            extendedData.add(extendedDataMutation);
        }

        public void addExtendedDataDeleteMutation(ExtendedDataDeleteMutation m) {
            extendedDataDeletes.add(m);
        }

        public void addExtendedDataRowDeleteMutation(ElementMutationBase.DeleteExtendedDataRowData m) {
            extendedDataRowDeletes.add(m);
        }

        public void addAdditionalExtendedDataVisibilityAddMutation(AdditionalExtendedDataVisibilityAddMutation m) {
            additionalExtendedDataVisibilities.add(m);
        }

        public void addAdditionalExtendedDataVisibilityDeleteMutation(AdditionalExtendedDataVisibilityDeleteMutation m) {
            additionalExtendedDataVisibilityDeletes.add(m);
        }
    }
}
