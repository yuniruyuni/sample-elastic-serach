FROM docker.elastic.co/elasticsearch/elasticsearch:7.13.3
RUN elasticsearch-plugin install analysis-kuromoji