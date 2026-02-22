# Architecture: NiFi Oil & Gas Upstream Monitoring Platform

## 1. System Overview

This document describes the end-to-end architecture of a production-grade Apache NiFi streaming platform designed for real-time IoT sensor monitoring on offshore oil & gas platforms.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      5 Simulated Offshore Platforms                         │
│    Alpha (GoM)  ·  Bravo (North Sea)  ·  Charlie (Santos Basin)           │
│    Delta (Persian Gulf)  ·  Echo (Campos Basin)                            │
│    250+ sensors  ·  4 types  ·  4 anomaly patterns                        │
└──────────┬──────────────────────────────┬───────────────────────────────────┘
           │ MQTT (tcp://1883)            │ REST (POST /api/sensors)
           ▼                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Apache NiFi 2.8.0                                   │
│                                                                             │
│  ┌────────────┐   ┌────────────┐   ┌────────────┐   ┌────────────┐        │
│  │ PG-01      │──▶│ PG-02      │──▶│ PG-03      │──▶│ PG-04      │        │
│  │ Ingestion  │   │ Validation │   │ Enrichment │   │ Routing    │        │
│  └────────────┘   └─────┬──────┘   └────────────┘   └──────┬─────┘        │
│                         │ DLQ                               │              │
│  ┌────────────┐   ┌─────▼──────┐   ┌────────────┐   ┌──────▼─────┐        │
│  │ PG-07      │   │ PG-05      │──▶│ PG-06      │──▶│ PG-09      │        │
│  │ Kafka Pub  │   │ Transform  │   │ Anomaly Det│   │ Alerting   │        │
│  └──────┬─────┘   └────────────┘   └────────────┘   └────────────┘        │
│         │                                                                   │
│  ┌──────▼─────┐   ┌────────────┐                                           │
│  │ PG-08      │   │ PG-10      │                                           │
│  │ Persistence│   │ Monitoring │                                           │
│  └──────┬─────┘   └────────────┘                                           │
└─────────┼───────────────────────────────────────────────────────────────────┘
          │
    ┌─────┼──────────────────┬──────────────────┬──────────────┐
    ▼     ▼                  ▼                  ▼              ▼
┌───────┐ ┌──────────────┐ ┌───────────────┐ ┌──────────┐ ┌─────────┐
│ Kafka │ │ TimescaleDB  │ │ PostgreSQL 16 │ │ MinIO    │ │Grafana  │
│ 3.7.1 │ │ (PG16)       │ │ (Ref Data)    │ │ (S3)     │ │ 11.4.0  │
│ KRaft │ │ Hypertables  │ │ Equipment     │ │ Raw Data │ │ 4 Dash  │
│7 topic│ │ Cont. Aggs   │ │ Alert History │ │ Parquet  │ │ RT 5s   │
└───────┘ └──────────────┘ └───────────────┘ └──────────┘ └─────────┘
```

---

## 2. Component Architecture

### 2.1 Data Source Layer (Simulated)

The **data-generator** Python package simulates 5 offshore platforms with 250+ sensors. Each sensor produces readings at configurable intervals (default 2s) via MQTT and REST protocols.

| Platform | Location | Sensors | Equipment | Type |
|----------|----------|---------|-----------|------|
| Alpha Station | Gulf of Mexico | 55 | 11 | Deep-water Drilling |
| Bravo Platform | North Sea | 48 | 10 | Production & Separation |
| Charlie FPSO | Santos Basin | 52 | 10 | Floating Production |
| Delta Jack-up | Persian Gulf | 45 | 9 | Shallow Water |
| Echo Semi-sub | Campos Basin | 50 | 10 | Semi-submersible |

**Sensor Types:** Temperature, Pressure, Vibration, Flow Rate (4 subtypes each).

**Anomaly Patterns:**
- **Normal (80%):** Gaussian noise around setpoint with seasonal drift
- **Degradation (10%):** Gradual linear drift simulating equipment wear
- **Failure (5%):** Sudden spike simulating equipment failure
- **Seasonal (5%):** 24h sinusoidal variation (ambient conditions)

### 2.2 Stream Processing Layer (Apache NiFi 2.8.0)

NiFi is the central data flow engine. The platform uses 10 Process Groups (PGs), each responsible for a single concern in the data pipeline:

#### PG-01: Ingestion
- **Processors:** ConsumeMQTT, ListenHTTP, GetFile
- **NiFi Features:** Back Pressure (10,000 FlowFiles / 100 MB), Load Balancing
- **Input:** Raw sensor JSON from MQTT topics (`platforms/+/sensors/+/readings`) and REST POST
- **Output:** Raw FlowFiles to PG-02

#### PG-02: Validation
- **Processors:** ValidateRecord (Avro), RouteOnAttribute
- **NiFi Features:** Dead Letter Queue, Confluent Schema Registry integration
- **Behavior:** Validates against `sensor-reading.avsc`, routes invalid records to DLQ (MinIO `sensor-dlq/` bucket)
- **Output:** Validated FlowFiles to PG-03

#### PG-03: Enrichment
- **Processors:** LookupRecord, UpdateRecord
- **NiFi Features:** DBCPConnectionPool (PostgreSQL), DistributedMapCacheClient
- **Behavior:** Joins sensor readings with equipment metadata (name, type, platform, location) from PostgreSQL reference data
- **Output:** Enriched FlowFiles to PG-04

#### PG-04: Routing
- **Processors:** RouteOnAttribute, RouteOnContent, Funnel
- **NiFi Features:** Priority-based routing, content-based routing
- **Behavior:** Routes by sensor type (temperature, pressure, vibration, flow_rate) and severity level
- **Output:** Routed FlowFiles to PG-05/PG-06/PG-07

#### PG-05: Transformation
- **Processors:** ConvertRecord (JSON→Avro), QueryRecord (SQL), MergeContent
- **NiFi Features:** Schema evolution (Avro), micro-batching (5s windows, 100 record min)
- **Behavior:** Converts to Avro, runs SQL aggregations (1-minute windows), merges into batches
- **Output:** Avro batches to PG-07, PG-08

#### PG-06: Anomaly Detection
- **Processors:** ThresholdDetector, MovingAverageDetector, RateOfChangeDetector (Python)
- **NiFi Features:** Python Processor API (NiFi 2.x), Stateful processing
- **Behavior:** Three-stage anomaly detection pipeline:
  1. **ThresholdDetector:** Compare against absolute high/low limits
  2. **MovingAverageDetector:** Detect deviations from rolling mean (configurable window)
  3. **RateOfChangeDetector:** Detect rapid value changes (spikes/drops)
- **Output:** Anomaly-tagged FlowFiles to PG-09

#### PG-07: Kafka Producer
- **Processors:** PublishKafka (consolidated NiFi 2.x processor)
- **NiFi Features:** Exactly-once semantics, Schema Registry integration
- **Topics:** 7 Kafka topics (see Section 2.4)
- **Output:** Published to Kafka

#### PG-08: Persistence
- **Processors:** PutDatabaseRecord (TimescaleDB), PutS3Object (MinIO), PutSQL (PostgreSQL)
- **NiFi Features:** Multi-destination write, batch inserts (500 records)
- **Destinations:**
  - TimescaleDB: `sensor_readings` hypertable (time-series)
  - MinIO: Raw JSON + Parquet archives (`sensor-data/` bucket)
  - PostgreSQL: Alert history (`alert_history` table)

#### PG-09: Alerting
- **Processors:** InvokeHTTP (webhook), MergeContent, PutSQL
- **NiFi Features:** Deduplication (30-minute window), escalation rules
- **Behavior:** Deduplicates alerts, applies severity escalation rules, logs to PostgreSQL alert_history, sends webhooks
- **Output:** Alert notifications + audit trail

#### PG-10: Monitoring
- **Processors:** PrometheusReportingTask
- **NiFi Features:** Flow provenance, SLA monitoring
- **Behavior:** Exports NiFi metrics to Prometheus for Grafana dashboards

### 2.3 Event Backbone (Apache Kafka 3.7.1)

Kafka runs in **KRaft mode** (no ZooKeeper) with SASL/PLAIN authentication on the client listener.

| Topic | Partitions | Retention | Schema | Purpose |
|-------|-----------|-----------|--------|---------|
| `sensor.raw` | 5 | 7d | sensor-reading.avsc | Raw ingested readings |
| `sensor.validated` | 5 | 3d | sensor-validated.avsc | Schema-validated readings |
| `sensor.enriched` | 5 | 3d | sensor-enriched.avsc | Metadata-enriched readings |
| `sensor.aggregated` | 3 | 30d | sensor-enriched.avsc | 1-minute aggregations |
| `alerts.critical` | 1 | 90d | anomaly-alert.avsc | Critical anomaly alerts |
| `equipment.status` | 5 | 30d | equipment-status.avsc | Equipment health updates |
| `compliance.emissions` | 3 | 365d | compliance-emission.avsc | Emission measurements |

**Schema Management:** Confluent Schema Registry 7.7.1 with BACKWARD compatibility. All schemas registered at startup via `register-schemas.sh`.

### 2.4 Storage Layer

#### TimescaleDB (Time-Series)
- **Engine:** TimescaleDB 2.x on PostgreSQL 16
- **Hypertable:** `sensor_readings` partitioned by `timestamp` (1-day chunks)
- **Continuous Aggregates:**
  - `sensor_1min_agg` (1-minute buckets, 2h refresh offset)
  - `sensor_5min_agg` (5-minute buckets, 6h refresh offset)
  - `sensor_15min_agg` (15-minute buckets, 1d refresh offset)
- **Retention Policies:**
  - Raw data: 90 days
  - 1-minute aggregates: 180 days
  - 5-minute aggregates: 365 days
  - 15-minute aggregates: Infinite

#### PostgreSQL 16 (Reference Data)
- **Tables:** `platforms`, `equipment`, `sensors`, `sensor_thresholds`, `alert_history`, `alert_escalation_rules`
- **Seed Data:** 5 platforms, ~50 equipment pieces, ~250 sensors, 12 threshold definitions, 6 escalation rules
- **Roles:** `nifi` (read/write), `grafana_reader` (read-only)

#### MinIO (S3-Compatible Data Lake)
- **Buckets:** `sensor-data`, `sensor-dlq`, `sensor-backups`
- **Content:** Raw JSON, Parquet archives, DLQ failed records, flow backups

### 2.5 Visualization Layer (Grafana 11.4.0)

Four dashboards provisioned at startup:

| Dashboard | Refresh | Panels | Data Source |
|-----------|---------|--------|-------------|
| Platform Overview | 10s | 5-platform grid, health scores, alert counts, throughput | TimescaleDB |
| Sensor Deep Dive | 5s | Time-series graph, anomaly markers, threshold bands | TimescaleDB |
| Alerts & Incidents | 15s | Alert timeline, severity heatmap, top failing equipment | PostgreSQL |
| Pipeline Health | 10s | NiFi throughput, Kafka lag, E2E latency, DB write rate | Prometheus |

### 2.6 Monitoring (Prometheus 2.54.0)

Prometheus scrapes metrics from:
- **NiFi** (HTTPS, TLS skip for self-signed cert) - port 8443
- **Kafka** - JMX exporter metrics
- **TimescaleDB** - pg_stat metrics

Retention: 15 days.

---

## 3. Data Flow

```
Sensor Reading Lifecycle:

  Generator → MQTT/REST → [PG-01 Ingestion]
                              │
                              ▼
                        [PG-02 Validation] ──DLQ──▶ MinIO sensor-dlq/
                              │
                              ▼
                        [PG-03 Enrichment] ◀── PostgreSQL (equipment metadata)
                              │
                              ▼
                        [PG-04 Routing]
                         /    |    \
                        ▼     ▼     ▼
              [PG-05]  [PG-06]  [PG-07 Kafka Pub]
              Transform Anomaly      │
                 │        │         ▼
                 │        ▼     Kafka (7 topics)
                 │    [PG-09 Alerting]
                 │        │
                 ▼        ▼
              [PG-08 Persistence]
              /       |        \
             ▼        ▼         ▼
        TimescaleDB  MinIO  PostgreSQL
                               (alerts)
             │
             ▼
          Grafana (4 dashboards)
```

**End-to-end latency target:** < 5 seconds from sensor reading to Grafana dashboard update.

---

## 4. Security Architecture

### 4.1 Credential Management
- **Zero hardcoded credentials** in all configuration files
- All secrets injected via environment variables from `.env.{profile}` files
- NiFi Parameter Contexts for flow-level secret management
- `.env.*` files excluded from version control via `.gitignore`

### 4.2 Network Security
- **Docker network isolation:** Single `oilgas-net` bridge network
- Only UI ports exposed to host (NiFi 8443, Grafana 3000, MinIO 9001)
- Inter-service communication on internal Docker network only

### 4.3 Authentication
- **NiFi:** HTTPS with auto-generated self-signed certificate, single-user auth
- **Kafka:** SASL/PLAIN on external CLIENT listener, PLAINTEXT on internal INTERNAL listener
- **PostgreSQL/TimescaleDB:** Password authentication via `POSTGRES_PASSWORD`
- **MinIO:** Root user/password via environment variables
- **Grafana:** Admin user/password, sign-up disabled

### 4.4 Authorization
- **Database least privilege:** Dedicated `nifi` and `grafana_reader` roles
- `grafana_reader`: SELECT-only on specific tables and views
- `nifi`: INSERT/UPDATE/SELECT on operational tables

### 4.5 Pre-commit Security
- **Gitleaks:** Secret scanning on every commit
- **detect-private-key:** Prevents accidental key commits

---

## 5. Deployment Architecture

### 5.1 Environments

| Environment | Profile | Configuration |
|-------------|---------|---------------|
| Development | `dev` | Full stack, debug ports, relaxed resource limits |
| Staging | `staging` | SSL enabled, stricter resource limits |
| Production | `prod` | Production simulation with replicas |

### 5.2 Docker Compose Services (12)

| # | Service | Image | Health Check |
|---|---------|-------|-------------|
| 1 | nifi | Custom (2.8.0) | `curl -kfs https://localhost:8443/nifi-api/system-diagnostics` |
| 2 | nifi-registry | apache/nifi-registry:1.28.1 | - |
| 3 | kafka | apache/kafka:3.7.1 | `kafka-broker-api-versions.sh` |
| 4 | kafka-init | apache/kafka:3.7.1 | One-shot (topic creation) |
| 5 | schema-registry | confluentinc/cp-schema-registry:7.7.1 | `curl http://localhost:8081/subjects` |
| 6 | postgres | postgres:16-alpine | `pg_isready` |
| 7 | timescaledb | timescale/timescaledb:latest-pg16 | `pg_isready` |
| 8 | minio | minio/minio:latest | `curl http://localhost:9000/minio/health/live` |
| 9 | minio-init | minio/mc:latest | One-shot (bucket creation) |
| 10 | grafana | grafana/grafana-oss:11.4.0 | - |
| 11 | prometheus | prom/prometheus:v2.54.0 | - |
| 12 | mosquitto | eclipse-mosquitto:2 | - |

### 5.3 Volume Strategy

All stateful services use named Docker volumes (`oilgas-{service}-data`) for data persistence across container restarts.

### 5.4 Service Dependency Chain

```
kafka ──▶ kafka-init ──▶ schema-registry
  │
  ├──▶ nifi (also depends on postgres, timescaledb)
  │
  └──▶ schema-registry

postgres ──▶ nifi
timescaledb ──▶ nifi, grafana
prometheus ──▶ grafana
mosquitto (independent)
minio ──▶ minio-init
```

---

## 6. CI/CD Pipeline

### 6.1 Continuous Integration (`ci.yml`)

```
Push/PR to main
      │
      ├── [lint] Ruff check + format (data-generator, processors, tests)
      ├── [yaml-lint] yamllint (docker-compose, prometheus, grafana, workflows)
      ├── [schema-validate] fastavro parse (6 Avro schemas)
      │
      └── [test] (needs: lint)
            pytest tests/unit/ --cov --junitxml
```

### 6.2 Deployment Pipelines

| Workflow | Trigger | Steps |
|----------|---------|-------|
| `deploy-dev.yml` | Manual | Health check → NiFi CLI export → Deploy → Validate |
| `promote-staging.yml` | Manual | Export dev flows → Import staging → Param swap → Integration tests |
| `promote-prod.yml` | Manual + approval | Export staging → Import prod → Canary monitor → Rollback ready |

### 6.3 NiFi Flow Management

Flows are managed via:
1. **NiFi Registry 1.28.1** for local version control (offline-first)
2. **NiFi CLI (Toolkit 2.8.0)** for export/import across environments
3. **REST API bootstrap** (`create-flow.py`) for initial flow creation

---

## 7. Avro Schema Strategy

All schemas live in `schemas/` as the single source of truth:

| Schema | Namespace | Records |
|--------|-----------|---------|
| `sensor-reading.avsc` | com.oilgas.sensor | Raw reading with quality flag |
| `sensor-validated.avsc` | com.oilgas.sensor | + validation_status, schema_version |
| `sensor-enriched.avsc` | com.oilgas.sensor | + equipment/platform metadata, geolocation |
| `equipment-status.avsc` | com.oilgas.equipment | Health score, maintenance tracking |
| `anomaly-alert.avsc` | com.oilgas.alert | Anomaly classification with severity |
| `compliance-emission.avsc` | com.oilgas.compliance | Emission measurements vs limits |

**Compatibility:** BACKWARD (Schema Registry enforced). New fields must have defaults.

---

## 8. Key Design Decisions

| # | Decision | Choice | Rationale |
|---|----------|--------|-----------|
| 1 | Stream Processor | NiFi 2.8.0 (not Flink/Spark) | Visual flow design, built-in processor library, Python Processor API |
| 2 | Kafka Mode | KRaft (no ZooKeeper) | Simplified ops, single binary, production-ready since Kafka 3.3 |
| 3 | Time-Series DB | TimescaleDB (not InfluxDB) | SQL compatibility, continuous aggregates, PostgreSQL ecosystem |
| 4 | Anomaly Detection | NiFi Python Processors | No external ML service, inline processing, NiFi 2.x native API |
| 5 | Schema Format | Avro (not JSON Schema/Protobuf) | NiFi native support, Schema Registry integration, compact binary |
| 6 | Data Lake | MinIO S3 (not HDFS) | Lightweight, S3-compatible, Docker-friendly, NiFi PutS3Object |
| 7 | Flow Versioning | NiFi Registry + REST API bootstrap | Offline-first, documented migration to GitHubFlowRegistryClient |
| 8 | Dashboards | Grafana (not Superset) | Native time-series support, real-time refresh, Prometheus integration |

---

## 9. Resource Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM | 12 GB | 16 GB |
| CPU | 4 cores | 8 cores |
| Disk | 10 GB | 20 GB |
| Docker | 24.0+ | Latest |

**NiFi JVM:** 1 GB init / 2 GB max (configurable via `NIFI_HEAP_INIT`/`NIFI_HEAP_MAX`)

---

## 10. Operational Runbook

### Start Platform
```bash
make start PROFILE=dev
```

### Check Health
```bash
make health
```

### Backup Flows
```bash
make backup
```

### Promote Flows
```bash
make promote FROM=dev TO=staging
```

### View Logs
```bash
make logs           # All services
make logs-nifi      # NiFi only
make logs-kafka     # Kafka only
```
