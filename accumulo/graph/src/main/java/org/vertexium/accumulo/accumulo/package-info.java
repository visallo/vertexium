/**
 * To update patch files run the following from the root of vertexium
 *
 * <pre>
 *   cp accumulo/graph/target/accumulo-source/org/apache/accumulo/core/client/impl/TabletServerBatchWriter.java \
 *     accumulo/graph/target/accumulo-source/org/apache/accumulo/core/client/impl/MultiTableBatchWriterImpl.java \
 *     accumulo/graph/target/generated-sources/org/vertexium/accumulo/accumulo/VertexiumMultiTableBatchWriter.java \
 *     accumulo/graph/target/generated-sources/org/vertexium/accumulo/accumulo/VertexiumTabletServerBatchWriter.java \
 *     accumulo/graph/src/main/java/org/vertexium/accumulo/accumulo
 * </pre>
 *
 * Make changes to VertexiumMultiTableBatchWriter.java or VertexiumTabletServerBatchWriter.java
 *
 * <pre>
 *     diff -u accumulo/graph/src/main/java/org/vertexium/accumulo/accumulo/MultiTableBatchWriterImpl.java \
 *       accumulo/graph/src/main/java/org/vertexium/accumulo/accumulo/VertexiumMultiTableBatchWriter.java \
 *       > accumulo/graph/src/main/java/org/vertexium/accumulo/accumulo/MultiTableBatchWriter.patch
 *     diff -u accumulo/graph/src/main/java/org/vertexium/accumulo/accumulo/TabletServerBatchWriter.java \
 *       accumulo/graph/src/main/java/org/vertexium/accumulo/accumulo/VertexiumTabletServerBatchWriter.java \
 *       > accumulo/graph/src/main/java/org/vertexium/accumulo/accumulo/TabletServerBatchWriter.patch
 *     rm accumulo/graph/src/main/java/org/vertexium/accumulo/accumulo/MultiTableBatchWriterImpl.java \
 *       accumulo/graph/src/main/java/org/vertexium/accumulo/accumulo/TabletServerBatchWriter.java \
 *       accumulo/graph/src/main/java/org/vertexium/accumulo/accumulo/VertexiumMultiTableBatchWriter.java \
 *       accumulo/graph/src/main/java/org/vertexium/accumulo/accumulo/VertexiumTabletServerBatchWriter.java
 * </pre>
 */
package org.vertexium.accumulo.accumulo;
