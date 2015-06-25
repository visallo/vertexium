1. Download [Titan with Hadoop 2](https://github.com/thinkaurelius/titan/wiki/Downloads):

        curl -L http://s3.thinkaurelius.com/downloads/titan/titan-0.5.4-hadoop2.zip -O

1. Extraxt the `.zip` file:

        jar -xf titan-*-hadoop2.zip

1. Add Vertexium and dependency `.jar` files to the Titan `lib` directory:

        cp dist/target/vertexium-dist-*/lib/vertexium-*.jar \
           dist/target/vertexium-dist-*/lib/accumulo-*.jar \
           dist/target/vertexium-dist-*/lib/cache2k-*.jar \
           titan-*-hadoop2/lib

1. Copy and edit the sample configuration files:

        cp conf/* titan-*-hadoop2/conf

        vi titan-*-hadoop2/accumulo-titan-hadoop.conf

1. Run the Gremlin shell:

        cd titan-*-hadoop2
        bin/gremlin.sh conf/startup.script

1. Test:

        g.V.has('conceptType', 'merchant').has('zipCode', '67226').vertexiumId
