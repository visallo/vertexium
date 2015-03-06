# Elasticsearch Parent/Child Index

This implementation of the index provides better security than the standard elasticsearch index. Each child
 document contains one field value as well as the visibility string. The visibility string is evaluated at the
 server and the child document is filtered using `AuthorizationsFilter`. The parent
 document contains the overall vertex/edge visibility as well as any boosting values (edge counts, etc).

## Why Parent/Child vs Nested?

Parent/child documents do not require the children to be re-indexed when a single child is changed. This allows us
to not store the data in Elasticsearch (only the tokenized values) and allows us to not elevate privileges to get all
the data needed to create the document. (see http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/parent-child.html)
