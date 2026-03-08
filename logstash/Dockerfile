# Base image from Elastic's official registry
FROM docker.elastic.co/logstash/logstash:8.10.2

# Remove default configuration files
RUN rm -f /usr/share/logstash/pipeline/logstash.conf

# Copy our custom configuration into the pipeline directory
COPY logstash.conf /usr/share/logstash/pipeline/