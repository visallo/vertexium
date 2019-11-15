package org.vertexium.elasticsearch7.lucene;

import org.vertexium.VertexiumException;

import java.io.StringReader;

public class LuceneQueryParser extends QueryParser {
    public LuceneQueryParser(String query) {
        super(new FastCharStream(new StringReader(query)));
    }

    public QueryStringNode parse() {
        try {
            return TopLevelQuery();
        } catch (ParseException e) {
            throw new VertexiumException("Failed to parse", e);
        }
    }

    public JJTQueryParserState getTree() {
        return jjtree;
    }

    @Override
    protected String discardEscapeChar(String str) {
        // TODO
        return str;
    }
}
