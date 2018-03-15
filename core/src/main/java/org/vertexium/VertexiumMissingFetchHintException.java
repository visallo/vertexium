package org.vertexium;

public class VertexiumMissingFetchHintException extends VertexiumException {
    private static final long serialVersionUID = 308097574790647596L;

    public VertexiumMissingFetchHintException(FetchHints fetchHints, String required) {
        super(createMessage(fetchHints, required));
    }

    private static String createMessage(FetchHints fetchHints, String required) {
        return String.format("Missing fetch hints. Found \"%s\" needed \"%s\"", fetchHints, required);
    }
}
