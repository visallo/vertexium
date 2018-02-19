package org.vertexium;

import java.util.EnumSet;

public enum IdFetchHint {
    INCLUDE_HIDDEN;

    public static final EnumSet<IdFetchHint> NONE = EnumSet.noneOf(IdFetchHint.class);
    public static final EnumSet<IdFetchHint> ALL_INCLUDING_HIDDEN = EnumSet.allOf(IdFetchHint.class);

    public static EnumSet<FetchHint> toFetchHints(EnumSet<IdFetchHint> idFetchHints) {
        return idFetchHints.contains(IdFetchHint.INCLUDE_HIDDEN) ? EnumSet.of(FetchHint.INCLUDE_HIDDEN) : FetchHint.NONE;
    }
}
