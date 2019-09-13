FROM elasticsearch:5.6.10

COPY elasticsearch.yml /usr/share/elasticsearch/config/elasticsearch.yml

COPY --chown=elasticsearch:elasticsearch \
   vertexium-elasticsearch5-plugin.zip \
   /tmp/vertexium-elasticsearch5-plugin.zip
RUN /usr/share/elasticsearch/bin/elasticsearch-plugin \
   install \
   file:///tmp/vertexium-elasticsearch5-plugin.zip
