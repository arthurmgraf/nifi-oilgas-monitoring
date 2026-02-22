# NiFi Oil & Gas Monitoring Platform - Custom NiFi 1.28.1 Image
# Adds: PostgreSQL JDBC driver, data directories, tuned logging.
# Build context: docker/nifi/

FROM apache/nifi:1.28.1

USER root

# 1. PostgreSQL JDBC Driver 42.7.4
ARG PG_JDBC_VERSION=42.7.4
RUN curl -fSL \
    "https://jdbc.postgresql.org/download/postgresql-${PG_JDBC_VERSION}.jar" \
    -o /opt/nifi/nifi-current/lib/postgresql-${PG_JDBC_VERSION}.jar \
    && chmod 644 /opt/nifi/nifi-current/lib/postgresql-${PG_JDBC_VERSION}.jar

# 2. Data import directory for file-based ingestion
RUN mkdir -p /data/import \
    && chown -R nifi:nifi /data/import

# 3. Copy custom configuration files
COPY --chown=nifi:nifi conf/bootstrap.conf /opt/nifi/nifi-current/conf/bootstrap.conf
COPY --chown=nifi:nifi conf/logback.xml    /opt/nifi/nifi-current/conf/logback.xml

# 4. Switch back to NiFi user
USER nifi

# 5. Health check (NiFi 1.x HTTP mode on 8080)
HEALTHCHECK --interval=30s --timeout=15s --start-period=300s --retries=5 \
    CMD curl -sf http://localhost:8080/nifi-api/system-diagnostics > /dev/null || exit 1

EXPOSE 8080
