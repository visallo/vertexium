This module supports two different types of serializers KryoVertexiumSerializer and QuickKryoVertexiumSerializer.

# KryoVertexiumSerializer

The KryoVertexiumSerializer uses Kryo's out of the box configuration. You can enable it with the following configuration:

```
serializer=org.vertexium.serializer.kryo.KryoVertexiumSerializer
```

# QuickKryoVertexiumSerializer

The QuickKryoVertexiumSerializer adds faster implementations for known Vertexium datatypes, it also supports compression
of the data. You can enable it with the following configuration:

```
serializer=org.vertexium.serializer.kryo.QuickKryoVertexiumSerializer
serializer.enableCompression=true
```
