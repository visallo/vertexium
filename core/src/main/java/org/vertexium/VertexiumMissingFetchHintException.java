package org.vertexium;

import java.util.EnumSet;

public class VertexiumMissingFetchHintException extends VertexiumException {
    private static final long serialVersionUID = 308097574790647596L;

    public VertexiumMissingFetchHintException(EnumSet<FetchHint> fetchHints, EnumSet<FetchHint> neededFetchHints) {
        super(createMessage(fetchHints, neededFetchHints));
    }

    private static String createMessage(EnumSet<FetchHint> fetchHints, EnumSet<FetchHint> neededFetchHints) {
        return "Missing fetch hints. Found \"" + fetchHints + "\" needed \"" + neededFetchHints + "\"";
    }
}
