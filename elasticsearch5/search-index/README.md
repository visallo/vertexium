# Elasticsearch

Each vertex/edge is stored in a single Elasticsearch document. Security is accomplished
by hashing the visibility string and appending it to the property name.