FROM registry.access.redhat.com/ubi9/ubi-minimal:latest

USER root

ARG OAUTH_LIB_VERSION=0.17.0
ARG NIMBUS_JOSE_JWT_VERSION=10.0.2
ARG JACKSON_ANNOTATION_VERSION=2.16.2
ARG JACKSON_DATABIND_VERSION=2.16.2
ARG JSON_PATH_VERSION=2.9.0

# Install Java, other necessary packages, and Kafka distribution
RUN microdnf update -y && \
    microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install -y \
    java-17-openjdk-headless shadow-utils tar gzip wget && \
    microdnf clean all && \
    wget https://dist.apache.org/repos/dist/dev/kafka/4.2.0-rc1/kafka_2.13-4.2.0.tgz -P /tmp && \
    mkdir -p /opt/kafka && \
    tar -xvzf /tmp/kafka_2.13-4.2.0.tgz -C /opt/kafka --strip-components=1 && \
    rm -f /tmp/kafka_2.13-4.2.0.tgz

# Download OAuth libraries
RUN echo "Including OAuth libraries in the base image..." && \
    OAUTH_COMMON_URL="https://repo1.maven.org/maven2/io/strimzi/kafka-oauth-common/${OAUTH_LIB_VERSION}/kafka-oauth-common-${OAUTH_LIB_VERSION}.jar" && \
    OAUTH_SERVER_URL="https://repo1.maven.org/maven2/io/strimzi/kafka-oauth-server/${OAUTH_LIB_VERSION}/kafka-oauth-server-${OAUTH_LIB_VERSION}.jar" && \
    OAUTH_SERVER_PLAIN_URL="https://repo1.maven.org/maven2/io/strimzi/kafka-oauth-server-plain/${OAUTH_LIB_VERSION}/kafka-oauth-server-plain-${OAUTH_LIB_VERSION}.jar" && \
    OAUTH_KEYCLOAK_AUTHORIZER_URL="https://repo1.maven.org/maven2/io/strimzi/kafka-oauth-keycloak-authorizer/${OAUTH_LIB_VERSION}/kafka-oauth-keycloak-authorizer-${OAUTH_LIB_VERSION}.jar" && \
    OAUTH_CLIENT_URL="https://repo1.maven.org/maven2/io/strimzi/kafka-oauth-client/${OAUTH_LIB_VERSION}/kafka-oauth-client-${OAUTH_LIB_VERSION}.jar" && \
    NIMBUS_JOSE_JWT_URL="https://repo1.maven.org/maven2/com/nimbusds/nimbus-jose-jwt/${NIMBUS_JOSE_JWT_VERSION}/nimbus-jose-jwt-${NIMBUS_JOSE_JWT_VERSION}.jar" && \
    # Jackson Libraries
    JACKSON_ANNOTATION_URL="https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/${JACKSON_ANNOTATION_VERSION}/jackson-annotations-${JACKSON_ANNOTATION_VERSION}.jar" && \
    JACKSON_DATABIND_URL="https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/${JACKSON_DATABIND_VERSION}/jackson-databind-${JACKSON_DATABIND_VERSION}.jar" && \
    # Json Path Library
    JSON_PATH_URL="https://repo1.maven.org/maven2/com/jayway/jsonpath/json-path/${JSON_PATH_VERSION}/json-path-${JSON_PATH_VERSION}.jar" && \
    # Download all JARs
    wget -P /opt/kafka/libs "$OAUTH_COMMON_URL" "$OAUTH_SERVER_URL" "$OAUTH_SERVER_PLAIN_URL" \
    "$OAUTH_KEYCLOAK_AUTHORIZER_URL" "$OAUTH_CLIENT_URL" "$NIMBUS_JOSE_JWT_URL" "$JACKSON_ANNOTATION_URL" "$JACKSON_DATABIND_URL" "$JSON_PATH_URL"

# Add strimzi user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN useradd -r -m -u 1001 -g 0 strimzi

#####
# Set JAVA_HOME env var
#####
ENV JAVA_HOME /usr/lib/jvm/jre-17
#####
# Add Kafka
#####
ENV KAFKA_HOME=/opt/kafka
#####
# Versions
#####
ENV KAFKA_VERSION=4.2.0
ENV SCALA_VERSION=2.13

WORKDIR $KAFKA_HOME

USER 1001
