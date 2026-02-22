# NiFi Oil & Gas Upstream Monitoring Platform

**Production-grade Apache NiFi 2.8 streaming platform** for real-time IoT sensor monitoring of offshore oil & gas platforms. Features 10 specialized NiFi Process Groups, Kafka event backbone, TimescaleDB time-series storage, anomaly detection via NiFi Python Processors, and Grafana real-time dashboards.

```
Sensors (250+) → MQTT/REST → NiFi (10 PGs) → Kafka (7 topics) → TimescaleDB → Grafana
                                  ↓                                    ↓
                           Anomaly Detection              Continuous Aggregates
                                  ↓                         (1m / 5m / 15m)
                           Alert Pipeline
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    5 Simulated Offshore Platforms                        │
│    Alpha (GoM)  Bravo (North Sea)  Charlie (Santos)  Delta  Echo       │
│     55 sensors    48 sensors        52 sensors      45     50          │
└──────────┬──────────────────────────────┬───────────────────────────────┘
           │ MQTT + REST                  │
           ▼                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Apache NiFi 2.8.0                                  │
│                                                                         │
│  PG-01 Ingestion ──▶ PG-02 Validation ──▶ PG-03 Enrichment            │
│       ↓                    ↓ (DLQ)              ↓                      │
│  PG-04 Routing ──▶ PG-05 Transform ──▶ PG-06 Anomaly Detection        │
│       ↓                    ↓                    ↓                      │
│  PG-07 Kafka Pub    PG-08 Persistence    PG-09 Alerting                │
│       ↓                    ↓                    ↓                      │
│                     PG-10 Monitoring (Prometheus)                       │
└──────┬─────────────────────┬────────────────────┬───────────────────────┘
       ▼                     ▼                    ▼
┌──────────────┐  ┌──────────────────┐  ┌──────────────┐  ┌────────────┐
│ Kafka 3.7    │  │ TimescaleDB      │  │ PostgreSQL   │  │ MinIO (S3) │
│ KRaft mode   │  │ Hypertables +    │  │ Reference +  │  │ Data Lake  │
│ 7 topics     │  │ Cont. Aggregates │  │ Alerts       │  │ Raw+Parquet│
│ Schema Reg.  │  └────────┬─────────┘  └──────────────┘  └────────────┘
└──────────────┘           │
                           ▼
                  ┌──────────────────┐
                  │ Grafana 11.4     │
                  │ 4 Dashboards     │
                  │ Real-time 5s     │
                  └──────────────────┘
```

---

## Tech Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Stream Processor | Apache NiFi | 2.8.0 | 10 Process Groups, Python Processors |
| Flow Versioning | NiFi Registry | 1.28.1 | Flow version control |
| Deployment CLI | NiFi Toolkit | 2.8.0 | CI/CD automation |
| Event Backbone | Apache Kafka | 3.7.1 (KRaft) | 7 topics, exactly-once, Avro |
| Schema Mgmt | Confluent Schema Registry | 7.7.1 | Avro schema evolution |
| Time-Series DB | TimescaleDB | 2.x (PG16) | Hypertables, continuous aggregates |
| Reference DB | PostgreSQL | 16 | Equipment metadata, alert history |
| Data Lake | MinIO | Latest | S3-compatible object storage |
| Dashboards | Grafana | 11.4.0 | 4 real-time dashboards |
| Metrics | Prometheus | 2.54.0 | NiFi + Kafka + system metrics |
| MQTT Broker | Eclipse Mosquitto | 2.0 | IoT sensor message broker |
| Data Simulator | Python | 3.11+ | 5 platforms, 250 sensors, anomaly patterns |
| CI/CD | GitHub Actions | - | Lint, test, deploy, promote |

---

## Quick Start

**Prerequisites:** Docker Desktop (16GB+ RAM allocated), Git, Make

```bash
# 1. Clone
git clone https://github.com/arthurmgraf/nifi-oilgas-monitoring.git
cd nifi-oilgas-monitoring

# 2. Setup (one-time)
make setup

# 3. Start
make start

# 4. Open dashboards
# NiFi:    https://localhost:8443/nifi/  (admin / dev_nifi_password_12chars)
# Grafana: http://localhost:3000         (admin / dev_grafana_pass_12c)
# MinIO:   http://localhost:9001         (minioadmin / dev_minio_password_12c)
```

---

## NiFi Process Groups (10)

| # | Process Group | Function | Key Processors | NiFi Features |
|---|--------------|----------|----------------|---------------|
| 01 | **PG-INGESTION** | Multi-protocol data acquisition | ConsumeMQTT, ListenHTTP, GetFile | Back Pressure, Load Balancing |
| 02 | **PG-VALIDATION** | Schema + range validation, DLQ | ValidateRecord, RouteOnAttribute | Dead Letter Queue, Schema Registry |
| 03 | **PG-ENRICHMENT** | Equipment metadata + geolocation | LookupRecord, UpdateRecord | DBCP, Cache Service |
| 04 | **PG-ROUTING** | Content-based routing by type/severity | RouteOnAttribute, RouteOnContent | Priority Routing, Funnel |
| 05 | **PG-TRANSFORMATION** | JSON→Avro, aggregation, normalization | ConvertRecord, QueryRecord, MergeContent | Schema Evolution, Micro-batching |
| 06 | **PG-ANOMALY-DETECTION** | Threshold + MA + rate-of-change | Python Processors (NiFi 2.x API) | Python Processor API, State Mgmt |
| 07 | **PG-KAFKA-PRODUCER** | Publish to 7 Kafka topics | PublishKafka (consolidated 2.x) | Exactly-once, Schema Registry |
| 08 | **PG-PERSISTENCE** | Write to TimescaleDB + MinIO + PG | PutDatabaseRecord, PutS3Object | Multi-destination, Batch Inserts |
| 09 | **PG-ALERTING** | Alert dedup, escalation, audit trail | InvokeHTTP, MergeContent, PutSQL | Deduplication, Webhook |
| 10 | **PG-MONITORING** | Flow metrics, SLA tracking | PrometheusReportingTask | Provenance, SLA Monitoring |

---

## Data Generator

Simulates 5 offshore platforms with realistic sensor patterns:

| Platform | Location | Sensors | Type |
|----------|----------|---------|------|
| Alpha Station | Gulf of Mexico | 55 | Deep-water Drilling |
| Bravo Platform | North Sea | 48 | Production & Separation |
| Charlie FPSO | Santos Basin | 52 | Floating Production |
| Delta Jack-up | Persian Gulf | 45 | Shallow Water |
| Echo Semi-sub | Campos Basin | 50 | Semi-submersible |

**Sensor Types:** Temperature (bearing, exhaust, ambient), Pressure (inlet, outlet, differential), Vibration (axial, radial, velocity), Flow Rate (rate, velocity, mass)

**Anomaly Patterns:**
- **Normal** (80%): Gaussian noise around setpoint with seasonal drift
- **Degradation** (10%): Gradual linear drift simulating wear
- **Failure** (5%): Sudden spike simulating equipment failure
- **Seasonal** (5%): 24h sinusoidal variation (ambient conditions)

---

## Kafka Topics

| Topic | Partitions | Retention | Schema |
|-------|-----------|-----------|--------|
| `sensor.raw` | 5 | 7 days | sensor-reading.avsc |
| `sensor.validated` | 5 | 3 days | sensor-validated.avsc |
| `sensor.enriched` | 5 | 3 days | sensor-enriched.avsc |
| `sensor.aggregated` | 3 | 30 days | sensor-enriched.avsc |
| `alerts.critical` | 1 | 90 days | anomaly-alert.avsc |
| `equipment.status` | 5 | 30 days | equipment-status.avsc |
| `compliance.emissions` | 3 | 365 days | compliance-emission.avsc |

---

## Grafana Dashboards

| Dashboard | Refresh | Content |
|-----------|---------|---------|
| **Platform Overview** | 10s | 5-platform grid, health scores, alert count, readings/sec |
| **Sensor Deep Dive** | 5s | Time-series graph, anomaly markers, threshold visualization |
| **Alerts & Incidents** | 15s | Alert timeline, severity distribution, top failing equipment |
| **Pipeline Health** | 10s | NiFi throughput, Kafka lag, E2E latency, DB write rate |

---

## Security

- **Zero hardcoded credentials** - All secrets via NiFi Parameter Contexts + `.env` files
- **NiFi HTTPS** with auto-generated self-signed certificate
- **Kafka SASL/PLAIN** authentication for all producer/consumer connections
- **Database SSL** on all JDBC connections
- **Docker network isolation** - Private `oilgas-net`, only UI ports exposed
- **Pre-commit hooks** with Gitleaks secret scanning
- **Least privilege** - Dedicated database users per service with minimal permissions

---

## CI/CD

| Workflow | Trigger | Steps |
|----------|---------|-------|
| `ci.yml` | Push to main | Ruff lint → YAML lint → Avro schema validation → pytest |
| `deploy-dev.yml` | Manual | NiFi CLI export → deploy → health check |
| `promote-staging.yml` | Manual | Export dev → import staging → param swap → integration tests |
| `promote-prod.yml` | Manual + approval | Export staging → import prod → canary monitor → rollback ready |

---

## Environments

```bash
make start PROFILE=dev       # Full stack, debug ports, relaxed limits
make start PROFILE=staging   # SSL enabled, stricter resource limits
make start PROFILE=prod      # Production simulation with replicas
```

---

## Make Targets

```bash
make setup          # One-time setup
make start          # Start platform (PROFILE=dev)
make stop           # Stop platform
make restart        # Restart all services
make health         # Check service health
make test           # Run unit tests
make test-all       # Run all tests (unit + integration + e2e)
make lint           # Run linters
make backup         # Create backup
make restore        # Restore from backup (BACKUP_DIR=...)
make promote        # Promote flows (FROM=dev TO=staging)
make logs           # Tail all logs
make clean          # Remove everything (DESTRUCTIVE)
make help           # Show all targets
```

---

## Project Structure

```
nifi-oilgas-monitoring/
├── docker-compose.yml            # 12 services with profiles (dev/staging/prod)
├── docker/
│   ├── nifi/                     # Custom NiFi 2.8.0 image + configs
│   ├── kafka/                    # Topic creation scripts
│   ├── schema-registry/          # Schema registration scripts
│   ├── timescaledb/init/         # Hypertables + continuous aggregates + retention
│   ├── postgresql/init/          # Reference data + alert tables + seed data
│   ├── grafana/                  # 4 dashboards + datasource provisioning
│   ├── prometheus/               # Scrape configuration
│   └── mosquitto/                # MQTT broker config
├── schemas/                      # 6 Avro schemas (source of truth)
├── data-generator/               # Python real-time sensor simulator
│   └── src/
│       ├── models/               # Platform, Sensor, Equipment, Reading
│       ├── generators/           # MQTT, REST, CSV publishers
│       └── patterns/             # Normal, degradation, failure, seasonal
├── nifi-python-processors/       # Custom NiFi 2.x Python Processors
│   ├── ThresholdDetector.py      # Threshold-based anomaly detection
│   ├── MovingAverageDetector.py  # Rolling window deviation detection
│   └── RateOfChangeDetector.py   # Spike detection
├── nifi-flows/bootstrap/         # Flow creation via NiFi REST API
├── scripts/                      # Operational scripts
│   ├── setup.sh / start.sh / stop.sh
│   ├── backup.sh / restore.sh / promote.sh
│   ├── health-check.sh
│   └── nifi-cli/                 # NiFi Toolkit CLI scripts
├── tests/                        # Unit + integration + E2E tests
├── .github/workflows/            # CI/CD pipelines (4 workflows)
├── docs/                         # Architecture docs
├── Makefile                      # All operational commands
└── .env.example                  # Environment variable template
```

---

## Resource Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM | 12 GB | 16 GB |
| CPU | 4 cores | 8 cores |
| Disk | 10 GB | 20 GB |
| Docker | 24.0+ | Latest |

---

## License

MIT

---

## Author

**Arthur Graf** - Staff Data Engineer

Built with Apache NiFi 2.8.0, Apache Kafka 3.7.1, TimescaleDB, and Grafana.
