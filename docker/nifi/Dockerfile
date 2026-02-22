# =============================================================================
# NiFi Oil & Gas Monitoring Platform - Custom NiFi 2.8.0 Image
# =============================================================================
# Adds: PostgreSQL JDBC driver, Python scientific libraries, data directories.
# Build context: docker/nifi/
# =============================================================================

FROM apache/nifi:2.8.0

USER root

# ---------------------------------------------------------------------------
# 1. PostgreSQL JDBC Driver 42.7.4
# ---------------------------------------------------------------------------
ARG PG_JDBC_VERSION=42.7.4
RUN curl -fSL \
    "https://jdbc.postgresql.org/download/postgresql-${PG_JDBC_VERSION}.jar" \
    -o /opt/nifi/nifi-current/lib/postgresql-${PG_JDBC_VERSION}.jar \
    && chmod 644 /opt/nifi/nifi-current/lib/postgresql-${PG_JDBC_VERSION}.jar

# ---------------------------------------------------------------------------
# 2. Python dependencies for NiFi Python Processors
#    NiFi 2.x ships its own Python env under /opt/nifi/nifi-current/python.
#    We install scientific libraries so that custom Python processors can
#    perform anomaly detection (scipy, numpy) directly inside the NiFi JVM.
# ---------------------------------------------------------------------------
RUN apt-get update -qq \
    && apt-get install -y --no-install-recommends \
       python3-pip \
       python3-dev \
       build-essential \
    && pip3 install --no-cache-dir --break-system-packages \
       numpy==1.26.4 \
       scipy==1.13.1 \
    && apt-get purge -y build-essential python3-dev \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# 3. Data import directory for file-based ingestion
# ---------------------------------------------------------------------------
RUN mkdir -p /data/import \
    && chown -R nifi:nifi /data/import

# ---------------------------------------------------------------------------
# 4. Copy custom configuration files (if present in build context)
# ---------------------------------------------------------------------------
COPY --chown=nifi:nifi conf/bootstrap.conf /opt/nifi/nifi-current/conf/bootstrap.conf
COPY --chown=nifi:nifi conf/logback.xml    /opt/nifi/nifi-current/conf/logback.xml

# ---------------------------------------------------------------------------
# 5. Switch back to NiFi user
# ---------------------------------------------------------------------------
USER nifi

# ---------------------------------------------------------------------------
# 6. Health check -- NiFi 2.x defaults to HTTPS on 8443
# ---------------------------------------------------------------------------
HEALTHCHECK --interval=30s --timeout=15s --start-period=120s --retries=5 \
    CMD curl -kfs https://localhost:8443/nifi-api/system-diagnostics > /dev/null || exit 1

EXPOSE 8443
