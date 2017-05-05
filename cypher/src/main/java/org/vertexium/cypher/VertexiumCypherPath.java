package org.vertexium.cypher;

import org.vertexium.Element;

import java.util.List;

public interface VertexiumCypherPath {
    String getPathName();

    List<Item> getItems();

    interface Item {
        String getItemName();

        Element getElement();
    }
}
